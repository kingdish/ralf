import asyncio
import hashlib
from abc import ABC, abstractmethod
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from queue import PriorityQueue
from typing import Callable, List, Optional
import threading

import psutil
import ray
from ray.actor import ActorHandle

from ralf.policies import load_shedding_policy, processing_policy
from ralf.state import Record, Schema, TableState

DEFAULT_STATE_CACHE_SIZE: int = 0

# This should represent a pool of sharded operators.
class ActorPool:
    def __init__(self, handles: List[ActorHandle]):
        self.handles = handles
        self._lazy = ray.get(handles[0].is_lazy.remote())

    @classmethod
    def make_replicas(cls, num_replicas, actor_class, *init_args, **init_kwargs):
        assert num_replicas > 0

        handles = [
            actor_class.options(max_concurrency=int(1e9)).remote(
                *init_args, **init_kwargs
            )
            for _ in range(num_replicas)
        ]
        for i, handle in enumerate(handles):
            handle.set_current_actor_handle.remote(handle)
            handle.set_shard_idx.remote(i)
        return cls(handles)

    def hash_key(self, key: str) -> int:
        # TODO: Figure out hashing for non-string keys
        hash_val = hashlib.sha1(key.encode("utf-8")).hexdigest()
        return int(hash_val, 16)

    def choose_actor(self, key) -> ActorHandle:
        return self.handles[self.hash_key(key) % len(self.handles)]

    # TODO: remove?
    def get(self, key):
        res = ray.get(self.choose_actor(key).get.remote(key))
        return res

    def get_async(self, key) -> ray.ObjectRef:
        return self.choose_actor(key).get.remote(key)

    def get_all_async(self) -> List[ray.ObjectID]:
        return [handle.get_all.remote() for handle in self.handles]

    def broadcast(self, attr, *args):
        # TODO: fix having to wait
        outputs = []
        for handle in self.handles:
            outputs.append(getattr(handle, attr).remote(*args))
        return outputs

    def is_lazy(self) -> bool:
        return self._lazy


class Event:
    """An event corresponding to a record that is processed by the operator.

    Orders events according to `processing_policy` if one is provided,
    otherwise orders events based on record processing time.
    """

    def __init__(
        self,
        work: Callable[[], None],
        record: Record,
        processing_policy: Callable[["Event", "Event"], bool],
        is_async: bool = False
    ):
        self._work = work
        self.record = record
        self._processing_policy = processing_policy
        self.is_async = is_async

    def __lt__(self, other) -> bool:
        return self._processing_policy(self.record, other.record)

    def __eq__(self, other) -> bool:
        return self._time == other._time

    def process(self):
        if self.is_async:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._work())
            loop.close()
        else:
            self._work()


class Operator(ABC):
    """Abstract Operator class.

    Transforms data from parent tables and stores results in an output table.
    Operators can compute lazily or eagerly, and manage queries to the output table.
    Computation can be multithreaded, as well as sharded by key across multiple
        processes.

    Args:
        schema: schema of the output table.
        cache_size: number of records stored in memory for the output table.
        lazy: whether records are produced lazily (on request) or eagerly.
        batch_size: number of events that will be popped from the event queue
            at a time. Events will be batch updated if possible (i.e. same key).
        num_worker_threads: number of concurrent threads with which recrods are
            produced.
        procesing_policy: a function that returns true if the first record should be
            processed before the second. By default, processes records in order of
            processing time.
        load_shedding_policy: decides whether to process the candidate record given that
            the current record is already present in the output table.
    """

    def __init__(
        self,
        schema: Schema,
        cache_size=DEFAULT_STATE_CACHE_SIZE,
        lazy: bool = False,
        batch_update_size: int = 1,
        num_worker_threads: int = 4,
        processing_policy: Callable[[Record, Record], bool] = processing_policy.fifo,
        load_shedding_policy: Callable[
            [Record, Record], bool
        ] = load_shedding_policy.always_process,
    ):

        # Mained output table state
        self._table = TableState(schema)
        self._cache_size = cache_size
        self._lru = OrderedDict()
        self._lazy = lazy
        self._batch_update_size = batch_update_size
        self._events = PriorityQueue()
        self._running = True
        self._processing_policy = processing_policy
        self._load_shedding_policy = load_shedding_policy
        if not self._lazy:
            self._thread_pool = ThreadPoolExecutor(num_worker_threads)
            for _ in range(num_worker_threads):
                self._thread_pool.submit(self._worker)
        else:
            self._thread_pool = ThreadPoolExecutor(num_worker_threads + 1) # extra thread for retrieving initial value
            for _ in range(num_worker_threads):
                self._thread_pool.submit(self._worker)

        # Parent tables (source of updates)
        self._parents = []
        # Child tables (descendants who recieve updates)
        self._children = []

        self._actor_handle = None
        self._shard_idx = 0

        self.proc = psutil.Process()
        self.proc.cpu_percent()

    def set_shard_idx(self, shard_idx: int):
        self._shard_idx = shard_idx

    def _process_stat(self):
        return {
            "cpu_percent": self.proc.cpu_percent(),
            "memory_mb": self.proc.memory_info().rss / (1024 * 1024),
        }

    def debug_state(self):
        return {
            "table": self._table.debug_state(),
            "process": self._process_stat(),
            "cache_size": self._cache_size,
            "lazy": self._lazy,
            "thread_pool_size": self._thread_pool._max_workers,
            "queue_size": self._events.qsize(),
        }

    def _run_event(self, event):
        # print(f"Queue size: {self._events.qsize()}")
        if self._table.schema is not None:
            key = getattr(event.record, self._table.schema.primary_key)
            try:
                current_record = self._table.point_query(key)
                # print(current_record["create_time"])
                if self._load_shedding_policy(event.record, current_record):
                    event.process()
            except KeyError:
                event.process()
        else:
            event.process()

    def _worker(self):
        """Continuously processes events."""
        while self._running:
            if self._batch_update_size == 1:
                event = self._events.get()
                self._run_event(event)
            else:
                # NOT THREAD SAFE
                # TODO: Batch update:
                # 1. filter event by key?
                # 2. order event by key? Add a hashmap? Will break processing_policy?
                # 3. Pop top 10??? and put back???
                events = [self._events.get() for _ in range(self._batch_update_size) if not self._events.empty()]
                filtered_events = []
                filtered_events_to_index_map = {}
                for event in events:
                    if self._table.schema is not None:
                        key = getattr(event.record, self._table.schema.primary_key)
                        if key in filtered_events_to_index_map:
                            idx = filtered_events_to_index_map[key]
                            filtered_events[idx] = event
                        else:
                            filtered_events_to_index_map[key] = len(filtered_events)
                            filtered_events.append(event)
                    else:
                        filtered_events.append(event)
                
                # print(len(filtered_events))
                for event in filtered_events:
                    self._run_event(event)


                

    @abstractmethod
    def on_record(self, record: Record) -> Optional[Record]:
        pass

    def _on_record_helper(self, record: Record):
        result = self.on_record(record)
        if result is not None:
            if isinstance(result, list):  # multiple output values
                for res in result:
                    self.send(res)
            else:
                self.send(result)

    async def _on_record(self, record: Record):
        event = Event(
            lambda: self._on_record_helper(record), record, self._processing_policy
        )
        self._events.put(event)

    def send(self, record: Record):
        key = getattr(record, self._table.schema.primary_key)
        # update state table
        self._table.update(record)

        # TODO(peter): move eviction code to an update_record function,
        # as the table may change lazily.
        if self._cache_size > 0:
            self._lru.pop(key, None)
            self._lru[key] = key
            self._table.update(record)

            if len(self._lru) > self._cache_size:
                evict_key = self._lru.popitem(last=False)[0]
                # Evict from parents
                for parent in self._parents:
                    parent.choose_actor(evict_key).evict.remote(evict_key)
                for child in self._children:
                    child.choose_actor(evict_key).evict.remote(evict_key)

        record._source = self._actor_handle
        # Network optimization: only send to non-lazy children.
        for child in filter(lambda c: not c.is_lazy(), self._children):
            child.choose_actor(key)._on_record.remote(record)

    def evict(self, key: str):
        self._table.delete(key)
        self._lru.pop(key, None)

    def set_parents(self, parents: List[ActorPool]):
        self._parents = parents

    def set_children(self, children: List[ActorPool]):
        self._children = children

    def get_children(self) -> List[ActorPool]:
        return self._children

    def get_parents(self) -> List[ActorPool]:
        return self._parents

    def is_lazy(self) -> bool:
        return self._lazy

    def set_current_actor_handle(self, actor_handle: ActorHandle):
        self._actor_handle = actor_handle

    def get_schema(self) -> Schema:
        return self._table.schema

    # Table data query functions
    async def _update_record(self, key: str):
        # Bug: stateful operators that produce output dependent on an
        # ordered lineage of parent records.
        parent_records = await asyncio.gather(
            *[parent.get_async(key) for parent in self.get_parents()]
        )

        # Force the thread pool to quickly service the requests.
        # TODO: submit via the events queue to prioritize requests.
        futures = []
        for parent_record in parent_records:
            task = self._thread_pool.submit(self._on_record_helper, parent_record)
            futures.append(asyncio.wrap_future(task))
        await asyncio.gather(*futures)

    async def get(self, key: str):
        if self._lazy:
            try:
                record = self._table.point_query(key)
                event = Event(
                    lambda: self._update_record(key), record, self._processing_policy, is_async=True
                )
                self._events.put(event)
                return record
            except KeyError:
                await self._update_record(key)
                record = self._table.point_query(key)
                return record
        else:
            record = self._table.point_query(key)
            # print("recorddd", record)
            # print("keyyy", record.json()['key'])
            # print("valuee", record['value'])
            return record

    def get_all(self):
        # TODO: Generate missing values
        return self._table.bulk_query()

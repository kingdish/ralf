import time
from typing import List, Optional

import pytest
import ray
from ray.util.queue import Empty, Queue

from ralf.core import Ralf
from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.operators.source import Source
from ralf.policies import load_shedding_policy, processing_policy
from ralf.state import Record, Schema
from ralf.table import Table
from ralf.client import RalfClient

@ray.remote
class CounterSource(Source):
    def __init__(self, send_up_to: int):
        self.count = 0
        self.send_up_to = send_up_to

        super().__init__(
            schema=Schema(
                "key", {
                    "key": str, 
                    "value": int,
                    "create_time": float
                }),
            cache_size=DEFAULT_STATE_CACHE_SIZE,
        )

    def next(self) -> Record:
        time.sleep(1)
        if self.count == 0:
            # Wait for downstream operators to come online.
            time.sleep(0.2)
        self.count += 1
        if self.count > self.send_up_to:
            raise StopIteration()
        return [Record(
            key=str(self.count % 3), 
            value=self.count,
            create_time=time.time()
        )]


# @ray.remote
# class Sink(Operator):
#     def __init__(self, result_queue: Queue):
#         super().__init__(schema=None, cache_size=DEFAULT_STATE_CACHE_SIZE)
#         self.q = result_queue

#     def on_record(self, record: Record) -> Optional[Record]:
#         self.q.put(record)
#         return record

@ray.remote
class SlowIdentity(Operator):
    def __init__(
        self,
        result_queue: Queue,
        processing_policy=processing_policy.fifo,
        load_shedding_policy=load_shedding_policy.always_process,
    ):
        super().__init__(
            schema=Schema("key", {"value": int}),
            cache_size=DEFAULT_STATE_CACHE_SIZE,
            num_worker_threads=1,
            processing_policy=processing_policy,
            load_shedding_policy=load_shedding_policy,
        )
        self.q = result_queue

    def on_record(self, record: Record) -> Optional[Record]:
        time.sleep(0.1)
        record = Record(
            key=record.key,
            value=record.value,
            create_time=record.create_time,
        )
        self.q.put(record)
        return record

    @classmethod
    def drop_smaller_values(cls, candidate: Record, current: Record):
        return candidate.value > current.value


def create_synthetic_pipeline(queue):

    # create Ralf instance
    # ralf_conn = Ralf(metric_dir=os.path.join(args.exp_dir, args.exp))
    # ralf_conn = Ralf(
    #     metric_dir=os.path.join(args.exp_dir, args.exp), log_wandb=True, exp_id=args.exp
    # )

    ralf = Ralf()
    
    # create pipeline
    source_table = Table([], CounterSource, 100)

    sink = source_table.map(
        SlowIdentity,
        queue
    ).as_queryable("sink")
    
    # deploy
    ralf.deploy(source_table, "source")
    ralf.deploy(sink, "sink")

    return ralf


def main():
    # create synthetic pipeline
    queue = Queue()
    ralf = create_synthetic_pipeline(queue)
    ralf.run()

    # snapshot stats
    run_duration = 20
    snapshot_interval = 2
    start = time.time()
    while time.time() - start < run_duration:
        snapshot_time = ralf.snapshot()
        remaining_time = snapshot_interval - snapshot_time
        if remaining_time < 0:
            print(
                f"snapshot interval is {snapshot_interval} but it took {snapshot_time} to perform it!"
            )
            time.sleep(0)
        else:
            print("writing snapshot", snapshot_time)
            # records: List[Record] = [queue.get() for _ in range(2)]
            # print([f"{record}: {record.latest_query_time - record.create_time}" for record in records])
            time.sleep(remaining_time)


if __name__ == "__main__":
    main()

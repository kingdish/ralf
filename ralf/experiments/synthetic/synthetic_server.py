import time
from typing import List, Optional

import ray
from ray.util.queue import Queue

from ralf.core import Ralf
from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.operators.source import Source
from ralf.policies import load_shedding_policy, processing_policy
from ralf.state import Record, Schema
from ralf.table import Table

import redis

SEND_UP_TO = 60000 # 1000000
NUM_KEYS = 1 # 3000
SEND_RATE = 1
RUN_DURATION = 60
LAZY = False
PROCESSING_TIME = 0.01
BATCH_UPDATE_SIZE = 1

@ray.remote
class CounterSource(Source):
    def __init__(self, send_up_to: int, num_keys: int, send_rate: float):
        self.count = 0
        self.send_up_to = send_up_to
        self.num_keys = num_keys
        self.send_rate = send_rate
        self.num_worker_threads = 4

        super().__init__(
            schema=Schema(
                "key", {
                    "key": str, 
                    "value": int,
                    "create_time": float
                }),
            cache_size=DEFAULT_STATE_CACHE_SIZE,
        )
        self.click_stream = redis.StrictRedis(
            host="localhost",
            port=8002,
            db=0,
            password=None,
        )

    def next(self) -> Record:
        time.sleep(1/SEND_RATE)
        while True:
            stream_data = self.click_stream.xreadgroup(
                "ralf-reader-group",
                consumername=f"reader-{self._shard_idx}",
                streams={"ralf": ">"},
                count=1,
                block=100 * 1000, # FIXME
            )
            if len(stream_data) > 0:
                break

        record_id, payload = stream_data[0][1][0]
        self.click_stream.xack("ralf", "ralf-reader-group", record_id)

        record_key = payload[b"key"].decode()
        record_key = str(record_key)
        record_value = payload[b"value"].decode()
        record_value = int(record_value)

        # print("yeeeeeee", record_key, record_value, "yeee")

        record = Record(
            key=record_key,
            value=record_value,
            create_time=time.time()
        )
        return [record]

@ray.remote
class SlowIdentity(Operator):
    def __init__(
        self,
        result_queue: Queue,
        processing_time: float,
        processing_policy=processing_policy.fifo,
        load_shedding_policy=load_shedding_policy.always_process,
        lazy=False,
        batch_update_size=1
    ):
        super().__init__(
            schema=Schema("key", {"value": int}),
            cache_size=DEFAULT_STATE_CACHE_SIZE,
            num_worker_threads=4,
            processing_policy=processing_policy,
            load_shedding_policy=load_shedding_policy,
            lazy=lazy,
            batch_update_size=batch_update_size
        )
        self.q = result_queue
        self.processing_time = processing_time

    def on_record(self, record: Record) -> Optional[Record]:
        time.sleep(self.processing_time)  # this is the artificial procesing time eg. featurization
        record = Record(
            key=record.key,
            value=record.value,
            create_time=record.create_time,
        )
        self.q.put(record)
        return record


def create_synthetic_pipeline(queue):
    ralf = Ralf()
    
    # create pipeline
    source_table = Table([], CounterSource, SEND_UP_TO, NUM_KEYS, SEND_RATE) 

    sink = source_table.map(
        SlowIdentity,
        queue,
        PROCESSING_TIME,
        lazy=LAZY,
        batch_update_size=BATCH_UPDATE_SIZE,
        # load_shedding_policy=load_shedding_policy.newer_processing_time
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
    run_duration = RUN_DURATION
    # snapshot_interval = 2
    start = time.time()
    while time.time() - start < run_duration:
        pass
        # snapshot_time = ralf.snapshot()
        # remaining_time = snapshot_interval - snapshot_time
        # if remaining_time < 0:
        #     print(
        #         f"snapshot interval is {snapshot_interval} but it took {snapshot_time} to perform it!"
        #     )
        #     time.sleep(0)
        # else:
        #     print("writing snapshot", snapshot_time)
        #     # records: List[Record] = [queue.get() for _ in range(2)]
        #     # print([f"{record}: {record.latest_query_time - record.create_time}" for record in records])
        #     time.sleep(remaining_time)


if __name__ == "__main__":
    main()

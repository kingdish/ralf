import time
from typing import List, Optional

import ray

from ralf.core import Ralf
from ralf.operator import DEFAULT_STATE_CACHE_SIZE, Operator
from ralf.operators.source import Source
from ralf.policies import load_shedding_policy, processing_policy
from ralf.state import Record, Schema
from ralf.table import Table

import redis

import asyncio
import traceback

# SEND_UP_TO = 60000 # 1000000
# NUM_KEYS = 1 # 3000
# SEND_RATE = 100
# RUN_DURATION = 90
# LAZY = False
# PROCESSING_TIME = 0.01
# BATCH_UPDATE_SIZE = 10

@ray.remote
class CounterSource(Source):
    def __init__(self, send_up_to: int, num_keys: int, update_rate: float):
        self.count = 0
        self.send_up_to = send_up_to
        self.num_keys = num_keys
        self.update_rate = update_rate
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

    async def _next(self):
        while True:
            try:
                await asyncio.sleep(1/self.update_rate)
                records = self.next()
            except Exception as e:
                if not isinstance(e, StopIteration):
                    traceback.print_exc()
                return
            # TODO(peter): optimize by adding batch send.
            for record in records:
                self.send(record)
            # Yield the coroutine so it can be queried.
            await asyncio.sleep(0)

    def next(self) -> Record:
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
        self.processing_time = processing_time

    def on_record(self, record: Record) -> Optional[Record]:
        time.sleep(self.processing_time)  # this is the artificial procesing time eg. featurization
        record = Record(
            key=record.key,
            value=record.value,
            create_time=record.create_time,
        )
        return record


def create_synthetic_pipeline(
    send_up_to,
    num_keys,
    update_rate,
    processing_time,
    is_lazy,
    batch_update_size
):
    ralf = Ralf()
    
    # create pipeline
    source_table = Table([], CounterSource, send_up_to, num_keys, update_rate) 

    sink = source_table.map(
        SlowIdentity,
        processing_time,
        lazy=is_lazy,
        batch_update_size=batch_update_size,
        # load_shedding_policy=load_shedding_policy.newer_processing_time
    ).as_queryable("sink")
    
    # deploy
    ralf.deploy(source_table, "source")
    ralf.deploy(sink, "sink")

    return ralf


def main(argv=None):
    send_up_to = argv.get("send_up_to", 100000)
    num_keys = argv.get("num_keys", 1)
    run_duration = argv.get("run_duration", 90)
    is_lazy = argv.get("is_lazy", False)
    processing_time = argv.get("processing_time", 0.01)
    batch_update_size = argv.get("batch_update_size", 1)
    update_rate = argv.get("update_rate", 10)

    # create synthetic pipeline
    ralf = create_synthetic_pipeline(
        send_up_to=send_up_to,
        num_keys=num_keys,
        update_rate=update_rate,
        processing_time=processing_time,
        is_lazy=is_lazy,
        batch_update_size=batch_update_size
    )
    ralf.run()

    # snapshot stats
    snapshot_interval = 30
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
            time.sleep(remaining_time)
    
    if 'result' in argv:
        argv['result'] = ralf.pipeline_view()["Table(SlowIdentity)"]['actor_state'][0]['table']['num_updates']
    # ralf.snapshot()


if __name__ == "__main__":
    main()

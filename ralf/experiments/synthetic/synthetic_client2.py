import json
import os
import re
import threading
import time
from glob import glob
from pathlib import Path
from typing import Dict

import msgpack
import pandas as pd
import redis
# from absl import app, flags
from ralf.client import RalfClient

from synthetic_server import SEND_UP_TO, NUM_KEYS, SEND_RATE, RUN_DURATION 

# FLAGS = flags.FLAGS
# flags.DEFINE_integer(
#     "redis_model_db_id", default=2, help="Redis DB number for db snapshotting."
# )
# flags.DEFINE_integer(
#     "redis_snapshot_interval_s", default=10, help="Interval for redis snapshotting."
# )
# flags.DEFINE_enum(
#     "workload", "yahoo_csv", ["yahoo_csv"], "Type of the workload to send"
# )
# flags.DEFINE_integer(
#     "send_rate_per_key", None, "records per seconds for each key", required=True
# )
# flags.DEFINE_string(
#     "yahoo_csv_glob_path", None, "the glob pattern to match all files", required=True
# )
# flags.DEFINE_string(
#     "yahoo_csv_key_extraction_regex",
#     None,
#     "extract regex from path name to int key",
# )
# flags.DEFINE_string(
#     "experiment_dir", None, "directory to write metadata to", required=True
# )
# flags.DEFINE_string("experiment_id", None, "experiment run name", required=True)


client = RalfClient()


def make_redis_producer():
    # r = redis.Redis()
    r = redis.StrictRedis(
            host="localhost",
            port=8003,
            db=0,
            password=None,
        )
    r.flushall()
    r.xgroup_create("ralf", "ralf-reader-group", mkstream=True)
    return r


# def snapshot_db_state(pkl_path):
#     r = redis.Redis(db=FLAGS.redis_model_db_id)
#     state = {key: r.dump(key) for key in r.keys("*")}
#     print(f"state size: {len(state)}")
#     # print(f"21 send time: {r.dumps('21/models/send_time')}")
#     with open(pkl_path, "wb") as f:
#         msgpack.dump(state, f)


# def _get_config() -> Dict:
#     """Return all the flag vlaue defined here."""
#     return {f.name: f.value for f in FLAGS.get_flags_for_module("__main__")}


        # self.click_stream = redis.StrictRedis(
        #     host="localhost",
        #     port=8002,
        #     db=0,
        #     password=None,
        # )
        # self.click_stream.flushall()
        # self.click_stream.xgroup_create("ralf", "ralf-reader-group", mkstream=True)
        # self.user_embedding = RalfClient()


def send_data():
    producer = make_redis_producer()
    for i in range(SEND_UP_TO):
        producer.xadd("ralf", {
            "key": i % NUM_KEYS,
            "value": i, 
        })

def query_data(i):
    record = client.point_query(key=i, table_name="sink")

def main():
    
    start_time = time.time()
    # last_snapshot_time = time.time()

    thread = threading.Thread(target=send_data)
    thread.start()

    # while time.time() - start_time < RUN_DURATION:
    for i in range(1,NUM_KEYS+1):
        start_query_time = time.time()
        #record = client.point_query(key=i, table_name="sink")
        thread = threading.Thread(target=query_data,args=(i,))
        thread.start()
        latency = time.time() - start_query_time
        staleness = time.time() - record['create_time']
        latency_arr.append(latency)
        staleness_arr.append(staleness)

    print("mean latency: ", mean(latency_arr))
    print("mean staleness: ", mean(staleness_arr))

    # for i in range(RUN_DURATION):
    #     send_start = time.time()

    #     producer.xadd("ralf", {
    #         "key": 1,
    #         "value": 1, # TODO: change key & value
    #     })
    #     send_duration = time.time() - send_start


        # should_sleep = 1 / FLAGS.send_rate_per_key - send_duration
        # # NOTE(simon): if we can't sleep this small, consider rewrite the client in rust.
        # if should_sleep < 0:
        #     print(
        #         f"Cannot keep up the send rate {FLAGS.send_rate_per_key}, it took "
        #         f"{send_duration} to send all the keys."
        #     )
        # else:
        #     time.sleep(should_sleep)

        # if time.time() - last_snapshot_time > FLAGS.redis_snapshot_interval_s:
        #     snapshot_relative_time = time.time() - start_time
        #     last_snapshot_time = time.time()
        #     pkl_path = exp_dir / f"client_dump-{snapshot_relative_time}.pkl"
        #     print("dumping to ", pkl_path)
        #     thread = threading.Thread(target=snapshot_db_state, args=(pkl_path,))
        #     thread.start()


if __name__ == "__main__":
    # app.run(main)
    main()
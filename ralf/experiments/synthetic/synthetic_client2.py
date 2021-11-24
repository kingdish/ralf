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
from ralf.client import RalfClient

from synthetic_server import SEND_UP_TO, NUM_KEYS, SEND_RATE, RUN_DURATION 

from statistics import mean
from concurrent.futures import ThreadPoolExecutor

client = RalfClient()


def make_redis_producer():
    # r = redis.Redis()
    r = redis.StrictRedis(
            host="localhost",
            port=8002,
            db=0,
            password=None,
        )
    r.flushall()
    r.xgroup_create("ralf", "ralf-reader-group", mkstream=True)
    return r


def send_data():
    producer = make_redis_producer()
    for i in range(SEND_UP_TO):
        producer.xadd("ralf", {
            "key": i % NUM_KEYS,
            "value": i, 
        })

def query_data(i, start_query_time, latency_arr, staleness_arr):
    record = client.point_query(key=i, table_name="sink")
    latency_arr[i] = time.time() - start_query_time
    staleness_arr[i] = time.time() - record['create_time']


def main():
    
    start_time = time.time()
    send_data()

    latency_arr = [0 for _ in range(NUM_KEYS)]
    staleness_arr = [0 for _ in range(NUM_KEYS)]
    threads = []
    for i in range(NUM_KEYS):
        start_query_time = time.time()
        thread = threading.Thread(target=query_data,args=(i,start_query_time,latency_arr, staleness_arr)) # get ~100 threads & send back the time after query 
        thread.start() 
        threads.append(thread)

    for t in threads:
        t.join()
    print("mean latency: ", mean(latency_arr))
    print("mean staleness: ", mean(staleness_arr))


if __name__ == "__main__":
    # app.run(main)
    main()
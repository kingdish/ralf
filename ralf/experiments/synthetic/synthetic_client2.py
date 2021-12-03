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

QUERY_RATE = 1000
QUERY_TIME = 5
NUM_RUN = 3

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
    data = [{"key": i % NUM_KEYS, "value": i} for i in range(SEND_UP_TO)]
    # for i in range(SEND_UP_TO):
    # for i in range(NUM_KEYS*20):
    for d in data:
        producer.xadd("ralf", d)

def query_data(i, start_query_time, latency_arr, staleness_arr):
    pre_query_time = time.time()
    record = client.point_query(key=i, table_name="sink")
    post_query_time = time.time()
    latency_arr.append(post_query_time - pre_query_time)
    staleness_arr.append(post_query_time - record['create_time'])

def scan_query():
    # latency_arr = [0 for _ in range(NUM_KEYS)]
    latency_arr = []
    staleness_arr = []
    threads = []
    for i in range(NUM_KEYS):
        start_query_time = time.time()
        thread = threading.Thread(target=query_data,args=(i,start_query_time,latency_arr, staleness_arr)) # get ~100 threads & send back the time after query 
        thread.start() 
        threads.append(thread)
        time.sleep(1/QUERY_RATE)

    for t in threads:
        t.join()
    
    return latency_arr, staleness_arr

def single_key_query(key):
    latency_arr = []
    staleness_arr = []
    threads = []
    for _ in range(int(QUERY_RATE * QUERY_TIME)):
        start_query_time = time.time()
        thread = threading.Thread(target=query_data,args=(key,start_query_time,latency_arr, staleness_arr)) # get ~100 threads & send back the time after query 
        thread.start()
        threads.append(thread)
        time.sleep(1/QUERY_RATE)

    for t in threads:
        t.join()
    
    return latency_arr, staleness_arr

def main():
    send_data()
    run_latency = []
    run_staleness = []
    for _ in range(NUM_RUN):
        latency_arr, staleness_arr = single_key_query(0)
        run_latency.append(mean(latency_arr))
        run_staleness.append(mean(staleness_arr))
        print("Run finished")
    print(f"mean latency: {mean(run_latency[1:])}; latency: {run_latency}")
    print(f"mean staleness: {mean(run_staleness[1:])}; staleness: {run_staleness}")


if __name__ == "__main__":
    main()
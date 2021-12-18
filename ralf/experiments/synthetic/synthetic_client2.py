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

# from synthetic_server import SEND_UP_TO, NUM_KEYS, SEND_RATE, RUN_DURATION 

from statistics import mean
from concurrent.futures import ThreadPoolExecutor

# QUERY_RATE = 10
# QUERY_TIME = 5
# NUM_RUN = 3

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


def send_data(num_keys, send_up_to):
    producer = make_redis_producer()
    data = [{"key": i % num_keys, "value": i} for i in range(send_up_to)]
    for d in data:
        producer.xadd("ralf", d)

def query_data(i, start_query_time, latency_arr, staleness_arr):
    pre_query_time = time.time()
    record = client.point_query(key=i, table_name="sink")
    post_query_time = time.time()
    latency_arr.append(post_query_time - pre_query_time)
    staleness_arr.append(post_query_time - record['create_time'])

def scan_query(num_keys, query_rate):
    latency_arr = []
    staleness_arr = []
    threads = []
    for i in range(num_keys):
        start_query_time = time.time()
        thread = threading.Thread(target=query_data,args=(i,start_query_time,latency_arr, staleness_arr)) # get ~100 threads & send back the time after query 
        thread.start() 
        threads.append(thread)
        time.sleep(1/query_rate)

    for t in threads:
        t.join()
    
    return latency_arr, staleness_arr

def single_key_query(key, query_rate, query_time):
    latency_arr = []
    staleness_arr = []
    threads = []
    for _ in range(int(query_rate * query_time)):
        start_query_time = time.time()
        thread = threading.Thread(target=query_data,args=(key,start_query_time,latency_arr, staleness_arr)) # get ~100 threads & send back the time after query 
        thread.start()
        threads.append(thread)
        time.sleep(1/query_rate)

    for t in threads:
        t.join()
    
    return latency_arr, staleness_arr

def main(argv=None):
    send_up_to = argv.get("send_up_to", 100000)
    num_keys = argv.get("num_keys", 1)
    query_rate = argv.get("query_rate", 10)
    query_time = argv.get("query_time", 5)
    num_runs = argv.get("num_runs", 3)
    
    send_data(num_keys=num_keys, send_up_to=send_up_to)
    run_latency = []
    run_staleness = []
    for _ in range(num_runs):
        latency_arr, staleness_arr = single_key_query(key=0, query_rate=query_rate, query_time=query_time)
        run_latency.append(mean(latency_arr))
        run_staleness.append(mean(staleness_arr))
        print("Run finished")
    print(f"mean latency: {mean(run_latency[1:])}; latency: {run_latency}")
    print(f"mean staleness: {mean(run_staleness[1:])}; staleness: {run_staleness}")

    if "result" in argv:
        argv["result"] = [mean(run_latency[1:]), mean(run_staleness[1:])]


if __name__ == "__main__":
    main()
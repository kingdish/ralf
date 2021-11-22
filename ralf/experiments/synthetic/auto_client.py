import sys
from tqdm import tqdm
import argparse
import os
import json
import time

from threading import Timer

import psutil

from ralf.client import RalfClient

import time
import random
from synthetic_server import SEND_UP_TO, NUM_KEYS, SEND_RATE, RUN_DURATION 
from statistics import mean

client = RalfClient()

if __name__ == "__main__":
    # record = client.point_query(key=5000, table_name="sink")
    # print(f"{record['key']} -> {record['value']}: {time.time() - record['create_time']}")

    # record = client.point_query(key=3, table_name="sink")
    # print(f"{record['key']} -> {record['value']}: {time.time() - record['create_time']}")

    latency_arr = []
    staleness_arr = []
    time.sleep(1)
    # for i in range(int((RUN_DURATION - (1/SEND_RATE*NUM_KEYS)) / 2)):
    for i in range(1, 7):
        time.sleep(0.00001)
        start_time = time.time()
        record = client.point_query(key=i, table_name="sink")
        latency = time.time() - start_time
        staleness = time.time() - record['create_time']
        latency_arr.append(latency)
        staleness_arr.append(staleness)
    print("mean latency: ", mean(latency_arr))
    print("mean staleness: ", mean(staleness_arr))


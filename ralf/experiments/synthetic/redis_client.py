import argparse
import os
import redis
import ray
from ray import serve
from ralf.client import RalfClient

import sys
from tqdm import tqdm
import json
import time
from threading import Timer
import psutil
import random
from synthetic_server import SEND_UP_TO, NUM_KEYS, SEND_RATE, RUN_DURATION 
from statistics import mean

serve.start(http_options={"port": 8001})

@serve.deployment
class InferenceServer:

    def __init__(
        self,
        send_up_to: int, num_keys: int, send_rate: float
    ):
        self.count = 0
        self.send_up_to = send_up_to
        self.num_keys = num_keys
        self.send_rate = send_rate
        self.num_worker_threads = 4

        self.click_stream = redis.StrictRedis(
            host="localhost",
            port=8002,
            db=0,
            password=None,
        )
        self.click_stream.flushall()
        self.click_stream.xgroup_create("ralf", "ralf-reader-group", mkstream=True)
        self.user_embedding = RalfClient()


    def send_data(self, key, value):
        self.click_stream.xadd("ralf", {
            "key": key,
            "value": value,
        })

    def counter(self, request):
        if self.count == 0:
            # Wait for downstream operators to come online.
            time.sleep(0.2)
        self.count += 1
        if self.count > self.send_up_to:
            raise StopIteration()
        # return [Record(
        #     key=str(self.count % self.num_keys), 
        #     value=self.count,
        #     create_time=time.time()
        # )]
        key = str(self.count % self.num_keys)
        value = self.count
        self.send_data(key, value)

        user_info = point_query(key=1, table_name="sink")

        return user_info


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    InferenceServer.deploy(
        send_up_to=SEND_UP_TO, num_keys=NUM_KEYS, send_rate=SEND_RATE)

    while True:
        pass
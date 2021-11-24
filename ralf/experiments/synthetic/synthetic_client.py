import sys
from tqdm import tqdm
import argparse
import os
import json
import time

from threading import Timer

import psutil

from ralf.client import RalfClient

client = RalfClient()

if __name__ == "__main__":
    results = []
    for _ in range(100):
        time.sleep(0.01)
        record = client.point_query(key=1, table_name="sink")
        result = f"{record['key']} -> {record['value']}: {time.time() - record['create_time']}"
        results.append(result)
    print(results)
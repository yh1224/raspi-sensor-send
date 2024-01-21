#!/usr/bin/env python3

import json
import os
import struct
import sys
import time
from datetime import datetime
from random import randint

from bluepy import btle
from newrelic_telemetry_sdk import MetricClient, GaugeMetric

import config

INTERVAL_SEC = 30
NEW_RELIC_INSERT_KEY = os.environ.get("NEW_RELIC_INSERT_KEY")


def get_sensor_data(mac_addr: str, model: str) -> dict:
    p = btle.Peripheral(mac_addr)
    svc = p.getServiceByUUID("fff0")
    ch = svc.getCharacteristics("fff2")[0]
    data = ch.read()
    p.disconnect()
    # print(list(data))
    (temp, humid) = struct.unpack("<hh", data[:4])
    return {
        "temperature": temp / 100,
        "humidity": humid / 100
    }


def send_sensor_data(sensor_data: dict, tags: dict):
    metric_client = MetricClient(NEW_RELIC_INSERT_KEY)
    res = metric_client.send_batch([
        GaugeMetric("temperature", sensor_data["temperature"], tags),
        GaugeMetric("humidity", sensor_data["humidity"], tags),
    ])
    res.raise_for_status()


def main():
    while True:
        wait_time = INTERVAL_SEC - datetime.now().timestamp() % INTERVAL_SEC
        print(f"Sleeping {wait_time:.2f} sec.")
        time.sleep(wait_time)
        for sensor in config.SENSORS:
            name = sensor["name"]
            mac_addr = sensor["sensor.macAddr"]
            model = sensor["sensor.model"]
            for i in range(3):  # retry
                try:
                    sensor_data = get_sensor_data(mac_addr, model)
                    print("{}: {} {} {}".format(datetime.now().isoformat(), mac_addr, name, json.dumps(sensor_data)))
                    send_sensor_data(sensor_data, sensor)
                    break
                except Exception as e:
                    print(f"{e.__class__.__name__}: {e}")
                    time.sleep(randint(1, 3))


if __name__ == "__main__":
    if NEW_RELIC_INSERT_KEY is None:
        print("ERROR: NEW_RELIC_INSERT_KEY not set.")
        sys.exit(1)
    main()

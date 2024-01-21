#!/usr/bin/env python3
import json
import os
import struct
import sys
from datetime import datetime
from typing import Optional

from bluepy.btle import Scanner, DefaultDelegate
from newrelic_telemetry_sdk import MetricClient, GaugeMetric

import config_govee

NEW_RELIC_INSERT_KEY = os.environ.get("NEW_RELIC_INSERT_KEY")


def get_sensor_data(data: bytes) -> Optional[dict]:
    i = 0
    while i < len(data):
        l = data[i]
        i += 1
        t = data[i]
        d = data[i + 1:i + l]
        i += l
        if t == 0xff and d[0] == 0x88 and d[1] == 0xec:
            th = struct.unpack(">l", d[2:6])[0]
            return {
                "temperature": int(th / 1000) / 10,
                "humidity": (th % 1000) / 10
            }
    return None


def send_sensor_data(sensor_data: dict, tags: dict):
    metric_client = MetricClient(NEW_RELIC_INSERT_KEY)
    res = metric_client.send_batch([
        GaugeMetric("temperature", sensor_data["temperature"], tags),
        GaugeMetric("humidity", sensor_data["humidity"], tags),
    ])
    res.raise_for_status()


class ScanDelegate(DefaultDelegate):
    def __init__(self):
        DefaultDelegate.__init__(self)

    def handleDiscovery(self, scanEntry, isNewDev, isNewData):
        for sensor in config_govee.SENSORS:
            name = sensor["name"]
            mac_addr = sensor["sensor.macAddr"]
            if mac_addr == scanEntry.addr:
                sensor_data = get_sensor_data(scanEntry.rawData)
                print("{}: {} {} {}".format(datetime.now().isoformat(), mac_addr, name, json.dumps(sensor_data)))
                send_sensor_data(sensor_data, sensor)
                break


def main():
    while True:
        scanner = Scanner().withDelegate(ScanDelegate())
        scanner.start()
        scanner.process(60)


if __name__ == "__main__":
    if NEW_RELIC_INSERT_KEY is None:
        print("ERROR: NEW_RELIC_INSERT_KEY not set.")
        sys.exit(1)
    main()

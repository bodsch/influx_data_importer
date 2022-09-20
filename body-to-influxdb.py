#!/usr/bin/env python3

import argparse

import os
import sys
import time
import datetime

import asyncio
from csv import DictReader

import reactivex as rx
from reactivex import operators as ops
from reactivex.scheduler.eventloop import AsyncIOScheduler

from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from pathlib import Path
import configparser

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def get_config_section(config_file, section):
    """
      read config file
    """
    config = configparser.RawConfigParser()
    config.read(config_file)

    return dict(config.items(section))

def convert_bool(obj):
    if isinstance(obj, bool):
        return obj
    if str(obj).lower() in ('no', 'false', '0'):
        return False
    if str(obj).lower() not in ('yes', 'true', '1'):
        raise ansible.errors.AnsibleConnectionFailure(
            f"expected yes/no/true/false/0/1, got {obj}"
        )

    return True

def parse_args():
    """
    """
    p = argparse.ArgumentParser(description='read csv file and try to push data in a influxdb')

    p.add_argument(
        "-f",
        "--file",
        required=False,
        help="csv file"
    )

    p.add_argument(
        "--owner",
        required=True,
        help="data owner"
    )

    return p.parse_args()


def csv_to_generator(owner, csv_file_path):
    """
    Parse your CSV file into generator
    """
    for row in DictReader(open(csv_file_path, 'r')):

        csv_date = row['Time']
        dt = datetime.datetime.strptime(csv_date, "%d-%m-%Y %H:%M:%S")

        _time = dt.strftime("%Y-%m-%d %H:%M:%S")

        time_stamp = int(
            time.mktime(
                datetime.datetime.strptime(csv_date, "%d-%m-%Y %H:%M:%S").timetuple()
            )
        )

        print(f"   - {_time}: time_stamp  {time_stamp}, weight: {row['Weight']}")

        point = Point('health') \
            .tag('owner', owner) \
            .field('weight', float(row['Weight'])) \
            .field('bmi', float(row['BMI'])) \
            .field('body_fat', float(row['Body Fat(%)'])) \
            .field('muscle_mass', float(row['Muscle Mass'])) \
            .field('visceral_fat', float(row['VisceralFat'])) \
            .field('body_water', float(row['Body Water(%)'])) \
            .field('bone_mass', float(row['Bone Mass'])) \
            .field('bmr', float(row['BMR'])) \
            .field('body_score', float(row['body Score'])) \
            .field('protein_rate', float(row['Protein Rate'])) \
            .field('skeletal_muscle_rate', float(row['Skeletal Muscle Rate'])) \
            .field('subcutaneous_fat', float(row['Subcutaneous Fat'])) \
            .field('lean_body_mass', float(row['Lean Body Mass'])) \
            .time(_time)
        yield point


async def main(influx_config, csv_file, owner):
    """
    """
    influx_url    = influx_config.get("url", None)
    influx_token  = influx_config.get("token", None)
    influx_org    = influx_config.get("org", None)
    influx_bucket = influx_config.get("bucket", None)
    influx_verbose = convert_bool(influx_config.get("verbose", False))

    if not influx_url or not influx_token or not influx_org or not influx_bucket:
        print("The configuration for InfluxDB is incomplete!")
        print("Please specify url, token, org and bucket.")
        sys.exit(1)

    async with InfluxDBClientAsync(url=influx_url, token=influx_token, org=influx_org) as client:
        """
        """
        write_api = client.write_api()

        """
        Async write
        """
        async def async_write(batch):
            """
            Prepare async task
            """
            await write_api.write(bucket=influx_bucket, record=batch)
            return batch

        """
        Prepare batches from generator
        """
        batches = rx \
            .from_iterable(
                csv_to_generator(owner, csv_file)
            ) \
            .pipe(ops.buffer_with_count(500)) \
            .pipe(ops.map(lambda batch: rx.from_future(asyncio.ensure_future(async_write(batch)))), ops.merge_all())

        done = asyncio.Future()

        """
        Write batches by subscribing to Rx generator
        """
        batches.subscribe(
            on_next=lambda batch: print(f'Written batch... {len(batch)}'),
            on_error=lambda ex: print(f'Unexpected error: {ex}'),
            on_completed=lambda: done.set_result(0),
            scheduler=AsyncIOScheduler(asyncio.get_event_loop())
        )

        """
        Wait to finish all writes
        """
        await done


if __name__ == "__main__":
    """
    """
    file_name   = Path(os.path.basename(__file__))
    config_name = file_name.with_suffix('.yml')
    cfg_file    = None

    cfg_locations = [ "/etc", f"{Path.home()}/.config", str(Path.cwd()) ]

    for path in cfg_locations:
        """
        """
        cfg_file = f"{str(path)}/{str(config_name)}"
        if( os.path.isfile( cfg_file ) ):
            break
        else:
            cfg_file = None

    print( f"use config file {cfg_file}")

    if not cfg_file:
        print( "ERROR: no config file found" )
        print(f"you can put on in this directories: {cfg_locations}")
        sys.exit(1)

    args = {}
    cli_args = parse_args()

    csv_file = cli_args.file
    owner = cli_args.owner

    influx_config = get_config_section(cfg_file, "influx")

    asyncio.run( main(influx_config, csv_file, owner) )

#!/usr/bin/env python3

import argparse

import time
import datetime

import asyncio
from csv import DictReader

import reactivex as rx
from reactivex import operators as ops
from reactivex.scheduler.eventloop import AsyncIOScheduler

from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

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


async def main(influx_url, influx_token, influx_org, influx_bucket, csv_file, owner):
    """
    """
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

    args = {}
    cli_args = parse_args()

    csv_file = cli_args.file
    owner = cli_args.owner

    influx_url = ""
    influx_token = ""
    influx_org = ""
    influx_bucket = "health"

    asyncio.run(
        main(
            influx_url,
            influx_token,
            influx_org,
            influx_bucket,
            csv_file,
            owner
        )
    )

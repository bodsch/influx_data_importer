#!/usr/bin/env python3

import sys
import os
import sqlite3
import argparse

import time
import datetime
import pandas as pd

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
    p = argparse.ArgumentParser(description='read openscal sqlite file and try to push data in a influxdb')

    p.add_argument(
        "-d",
        "--database",
        required=True,
        help="databasefile",
        default='openScale.db'
    )

    p.add_argument(
        "--owner",
        required=True,
        help="data owner"
    )

    return p.parse_args()

def convert_to_csv(database_file):
    """
    """
    conn = sqlite3.connect(
        database_file,
        isolation_level=None,
        detect_types=sqlite3.PARSE_COLNAMES
    )

    query = "SELECT datetime,weight,fat,water,muscle,visceralFat,lbm,waist,hip,bone FROM scaleMeasurements where enabled = '1' order by datetime"
    # query = "SELECT * FROM scaleMeasurements where enabled = '1' order by datetime"

    db_df = pd.read_sql_query(query, conn)
    db_df.to_csv('database.csv', index=False)

def bmi(m,l):
    return m / (l ** 2)

def csv_to_generator(owner, csv_file_path):
    """
    Parse your CSV file into generator
    """
    for row in DictReader(open(csv_file_path, 'r')):

        timestamp = int(row['datetime']) / 1000

        _time = datetime.datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

        # print("Date time object:", date_time)

        # dt = datetime.datetime.strptime(csv_date, "%d-%m-%Y %H:%M:%S")

        # _time = dt.strftime("%Y-%m-%d %H:%M:%S")

        time_stamp = int(
            time.mktime(
                datetime.datetime.strptime(_time, "%Y-%m-%d %H:%M:%S").timetuple()
            )
        )

        print(f"   - {_time}: time_stamp  {time_stamp}, weight: {row['weight']}")

        # 82/(1,70*1,70)
        _bmi = float(row['weight'])/((float(170)/100)**2)

        point = Point('health') \
            .tag('owner', owner) \
            .field('weight', float(row['weight'])) \
            .field('bmi', float( _bmi )) \
            .field('body_fat', float(row['fat'])) \
            .field('muscle_mass', float(row['muscle'])) \
            .field('visceral_fat', float(row['visceralFat'])) \
            .field('body_water', float(row['water'])) \
            .field('bone_mass', float(row['bone'])) \
            .field('lean_body_mass', float(row['lbm'])) \
            .time(_time)

        yield point



async def main(influx_url, influx_token, influx_org, influx_bucket, database_file, owner):
    """
    """
    if not os.path.exists(database_file):
        """
        """
        print(f"database file {database_file} dosen't exists")
        sys.exit(1)

    convert_to_csv(database_file)

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
                csv_to_generator(owner, 'database.csv')
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

    database_file = cli_args.database
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
            database_file,
            owner
        )
    )

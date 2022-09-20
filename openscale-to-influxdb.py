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



async def main(influx_config, database_file, owner):
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

    database_file = cli_args.database
    owner = cli_args.owner

    influx_config = get_config_section(cfg_file, "influx")

    asyncio.run( main(influx_config, database_file, owner) )

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
import logging

logging.basicConfig(
    level=logging.DEBUG) #, format='(%(threadName)-26s) %(message)s',)

log = logging.getLogger('notify2influxdb')
streamhandler = logging.StreamHandler(sys.stdout)



SCHEMAS = ["HeartMonitorData", "SleepData", "SleepDayData", "StepsData", "Workout", "ActivityData"]

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
    p = argparse.ArgumentParser(description='read notify4mi sqlite file and try to push data in a influxdb')

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
    log.debug(f"convert_to_csv({database_file})")

    conn = sqlite3.connect(
        database_file,
        isolation_level=None,
        detect_types=sqlite3.PARSE_COLNAMES
    )

    # schemas = []
    # query = "SELECT name FROM sqlite_schema WHERE type ='table' AND name not LIKE '%metadata%' and (name LIKE 'rush_%_miband1_model2_%' or name like '%Data')"

    # cursor = conn.execute(query)

    # for row in cursor:
    #     schemas.append(row[0])
    #
    # for r in schemas:
    #     print(f"  - {r}")
    #     query = f"select * from {r}"
    #     db_df = pd.read_sql_query(query, conn)
    #     db_df.to_csv(f'{r}.csv', index=False)

    # schemas = ["HeartMonitorData", "SleepData", "SleepDayData", "StepsData", "Workout", "ActivityData"]

    for s in SCHEMAS:
        log.debug(f"  - {s}")

        query = None

        if s == "HeartMonitorData":
            query = "select hidden,intensity,isActivityValue,isWorkout,timestamp from rush_com_mc_miband1_model2_HeartMonitorData where hidden = 'false' order by timestamp"

        # query = "SELECT intensity, ,weight,fat,water,muscle,visceralFat,lbm,waist,hip,bone FROM scaleMeasurements where enabled = '1' order by datetime"
        if query:
            db_df = pd.read_sql_query(query, conn)
            db_df.to_csv(f'{s}.csv', index=False)

        #csv_to_generator(owner, f'{r}.csv')

        # break

    # schemas = []
    #
    # sys.exit(1)
    #
    # query = "SELECT name FROM sqlite_schema WHERE type ='table' AND name not LIKE '%metadata%' and (name LIKE 'rush_%' or name like '%Data')"
    #
    # cursor = conn.execute(query)
    #
    # for row in cursor:
    #     schemas.append(row[0])
    #
    # for r in schemas:
    #     print(f"  - {r}")
    #     query = f"select * from {r}"
    #     db_df = pd.read_sql_query(query, conn)
    #     db_df.to_csv(f'{r}.csv', index=False)


def csv_to_generator(owner, schema, csv_file_path):
    """
    Parse your CSV file into generator
    """
    log.debug(f"csv_to_generator({owner}, {schema}, {csv_file_path})")

    for row in DictReader(open(csv_file_path, 'r')):
        """
        """
        # log.debug(row)

        # e.g.: 1564815561316
        timestamp = int(row['timestamp']) / 1000

        _time = datetime.datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

        time_stamp = int(
            time.mktime(
                datetime.datetime.strptime(_time, "%Y-%m-%d %H:%M:%S").timetuple()
            )
        )

        if schema.lower() == "heartmonitordata":
            """
            """
            log.debug(f"   - {_time}: time_stamp  {time_stamp}, {float(row['intensity'])}, workout: {convert_bool(row['isWorkout'])}, activity: {convert_bool(row['isActivityValue'])}")

            # continue

            point = Point('heartbeat') \
                .tag('owner', owner) \
                .tag("source", "mi-band") \
                .field('intensity', float(row['intensity'])) \
                .field('workout', convert_bool(row['isWorkout'])) \
                .field('activity', convert_bool(row['isActivityValue'])) \
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
        log.error("The configuration for InfluxDB is incomplete!")
        log.error("Please specify url, token, org and bucket.")
        sys.exit(1)

    if not os.path.exists(database_file):
        """
        """
        log.error(f"database file {database_file} dosen't exists")
        sys.exit(1)

    # convert_to_csv(database_file)

    for s in SCHEMAS:
        log.debug(f'  -> {s}.csv')

        # continue

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
                batch = None
                await write_api.write(bucket=influx_bucket, record=batch)
                return batch

            """
            Prepare batches from generator
            """
            batches = rx \
                .from_iterable(
                    csv_to_generator(owner, s, f'{s}.csv')
                ) \
                .pipe(ops.buffer_with_count(500)) \
                .pipe(ops.map(lambda batch: rx.from_future(
                    asyncio.ensure_future(async_write(batch)))), ops.merge_all()
                )

            done = asyncio.Future()

            """
            Write batches by subscribing to Rx generator
            """
            batches.subscribe(
                on_next=lambda batch: log.info(f'Written batch... {len(batch)}'),
                on_error=lambda ex: log.error(f'Unexpected error: {ex}'),
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

    log.debug( f"use config file {cfg_file}")

    if not cfg_file:
        log.error( "ERROR: no config file found" )
        log.error(f"you can put on in this directories: {cfg_locations}")
        sys.exit(1)

    args = {}
    cli_args = parse_args()

    database_file = cli_args.database
    owner = cli_args.owner

    if cfg_file:
        influx_config = get_config_section(cfg_file, "influx")
    else:
        influx_config = dict()

    asyncio.run( main(influx_config, database_file, owner), debug=True )

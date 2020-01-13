# coding: utf-8

from pyspark.sql.functions import UserDefinedFunction, lit
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException

import json
import time
from datetime import datetime, timedelta
from pytz import timezone
import sys, os, argparse


class MTime(object):
    def __init__(self, y_str, m_str, d_str, h_str, min_str, s_srt):
        self.year = y_str
        self.month = m_str
        self.day = d_str
        self.hour = h_str
        self.min = min_str
        self.second = s_srt


def get_last_hour_date(timezone_str='UTC'):
    now = datetime.now(timezone(timezone_str))
    last_hour_date = now - timedelta(hours=1)
    year, month, day = str(last_hour_date).split(' ')[0].split('-')
    hour, minn, second, _ = str(last_hour_date).split(' ')[1].split(':')
    last_hour_date = MTime(year, month, day, hour, minn, second)

    return last_hour_date


def datetime_str_to_mtime(datetime_str):
    year, month, day = str(datetime_str).split('_')[0].split('-')
    hour, minn, second = str(datetime_str).split('_')[1].split(':')
    return MTime(year, month, day, hour, minn, second)


def get_dataset_path(datasrc_s3, datetime_partition=get_last_hour_date()):
    """
        Reading dataset to partition (last one hour of data streamed by firehose)

        Args:
            - datasrc (str):  root path of s3 bucket
            - datetime_partition (MTime): get path on s3 based on hour of creation of objects
    """
    path = datasrc_s3 \
        + "/ingestion_year=" + str(datetime_partition.year) \
        + "/ingestion_month=" + str(datetime_partition.month) \
        + "/ingestion_day=" + str(datetime_partition.day) \
        + "/ingestion_hour=" + str(datetime_partition.hour)
    return path


### Creating partitions: Day, Month and Year

YEAR_INDEX = 0
MONTH_INDEX = 1
DAY_INDEX = 2

def getIntervalTime(creationDate, time_index, lastChange=None):
    """
        Extracts year, month or day information from datatime string.

        Args:
            - creationDate (str): datetime string of creation date of order.
            - lastChange (str): if exists, datetime string of last modification of order.
            - time_index (int): 0 for extracting year info, 1 for month and 2 for day.
    """
    date = lastChange if lastChange is not None else creationDate
    return date.split('T')[0].split('-')[time_index]

def create_partition_columns(df, use_last_change):
    """Create the Columns for the Partitions"""

    # Register functions as Spark UDFs
    udf_getIntervalTime = UserDefinedFunction(getIntervalTime, StringType())

    if(use_last_change):
        df = df.withColumn('YEAR', udf_getIntervalTime(df.creationdate, lit(YEAR_INDEX), df.lastchange))
        df = df.withColumn('MONTH', udf_getIntervalTime(df.creationdate, lit(MONTH_INDEX), df.lastchange))
        df = df.withColumn('DAY', udf_getIntervalTime(df.creationdate, lit(DAY_INDEX), df.lastchange))
    else:
        df = df.withColumn('YEAR', udf_getIntervalTime(df.creationdate, lit(YEAR_INDEX)))
        df = df.withColumn('MONTH', udf_getIntervalTime(df.creationdate, lit(MONTH_INDEX)))
        df = df.withColumn('DAY', udf_getIntervalTime(df.creationdate, lit(DAY_INDEX)))

    return df


def _read_args():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--destination-path',
        help='S3 URI where save parquet files', \
        default='s3://vtex.datalake/consumable_tables/'
    )
    parser.add_argument(
        '--use-last-change',
        help='Flag to use or not lastChange date',
        default="false"
    )
    parser.add_argument(
        '--datetime-partition',
        help="Partition objects created on this specific date and hour. " \
            + "If None, current last hour will be used. Format: yyyy-MM-dd_hh:mm:ss",
        default=None
    )
    parser.add_argument('--datasrc-s3')
    args=parser.parse_args()
    use_last_change = args.use_last_change == "true"

    return args.datasrc_s3, args.destination_path, use_last_change, args.datetime_partition


if __name__ == "__main__":
    datasrc_s3, destination_path, use_last_change, datetime_partition = _read_args()

    ### Config SparkContext
    spark = SparkSession \
        .builder \
        .appName("Partition") \
        .getOrCreate()

    if datetime_partition:
        datetime_partition = datetime_str_to_mtime(datetime_partition)
        datapath = get_dataset_path(datasrc_s3, datetime_partition)
    else:
        datapath = get_dataset_path(datasrc_s3)

    df = spark.read.parquet(datapath)
    df = create_partition_columns(df, use_last_change)

    #### Save table to S3 using Parquet format and partitioning by defined columns
    df \
        .repartition('YEAR','MONTH','DAY') \
        .write \
        .partitionBy(['YEAR','MONTH','DAY']) \
        .mode('append') \
        .parquet(destination_path)

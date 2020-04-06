# !/usr/bin/env python
# coding: utf-8

import argparse
import re
import time

from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_json, struct, col, size, explode, when, UserDefinedFunction, countDistinct, lit
from pyspark.sql.types import IntegerType, Row


spark = SparkSession \
    .builder \
    .appName('Generate csv for data orders on data mart src.') \
    .getOrCreate()

YEAR_INDEX = 0
MONTH_INDEX = 1
DAY_INDEX = 2

cols = [
    'id','hostname', 'shippingdata',
    'items','sellerorderid','ordergroup', 'creationdate',
    'origin','value','storepreferencesdata', 'iscompleted'
]

pkeys = [
    'id','subaccount','seller_order_id','order_group'
]


def format_datetime_str(datetime_str):
    datetime_pattern = '%Y-%m-%dT%H:%M:%S'
    datetime_str_without_UTC = re.split('\.|Z', datetime_str)[0]
    datetime_object = datetime.strptime(datetime_str_without_UTC, datetime_pattern)
    
    return str(datetime_object.replace(microsecond=0))

def getIntervalTime(creationDate, time_index, lastChange=None):
    """
        Extracts year, month or day information from datatime string.
        Args:
            - creationDate (str): datetime string of creation date of order.
            - lastChange (str): if exists, datetime string of last modification of order.
            - time_index (int): 0 for extracting year info, 1 for month and 2 for day.
    """
    date = lastChange if lastChange is not None else creationDate
    return date.split(' ')[0].split('-')[time_index]

def create_partition_columns(df, use_last_change):
    """Create the Columns for the Partitions"""
    # Register functions as Spark UDFs
    udf_getIntervalTime = UserDefinedFunction(getIntervalTime, StringType())
    if(use_last_change):
        df = df.withColumn('YEAR', udf_getIntervalTime(df.creation_date, lit(YEAR_INDEX), df.lastchange))
        df = df.withColumn('MONTH', udf_getIntervalTime(df.creation_date, lit(MONTH_INDEX), df.lastchange))
        df = df.withColumn('DAY', udf_getIntervalTime(df.creation_date, lit(DAY_INDEX), df.lastchange))
    else:
        df = df.withColumn('YEAR', udf_getIntervalTime(df.creation_date, lit(YEAR_INDEX)))
        df = df.withColumn('MONTH', udf_getIntervalTime(df.creation_date, lit(MONTH_INDEX)))
        df = df.withColumn('DAY', udf_getIntervalTime(df.creation_date, lit(DAY_INDEX)))
    return df

def write_df_to_csv(df, write_path):
    df.repartition('YEAR','MONTH','DAY') \
        .write \
        .partitionBy('YEAR','MONTH','DAY') \
        .format('csv') \
        .option("sep", ";") \
        .option("header", "true") \
        .mode('append') \
        .save(write_path)

def _read_args():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--s3-read-path',
        help='S3 path to be read the data. For example: vtex/test',
        required=True
    )
    parser.add_argument(
        '--s3-destination-path',
        help='Where the data to be written. For example: vtex/checkout',
        required=True
    )
    args=parser.parse_args()
    
    return args.s3_read_path, args.s3_destination_path


if __name__ == "__main__":
    s3_read_path, s3_destination_path = _read_args()

    basePath = 's3://vtex.datalake/raw_data/orders/parquet_stage/'
    df = spark.read.option("basePath", basePath).parquet(*[s3_read_path])

    select_df = df.select(*cols)

    format_datetime_str_UDF = UserDefinedFunction(format_datetime_str, StringType())
    select_df = select_df.withColumn('creation_date', format_datetime_str_UDF(select_df.creationdate))
    select_df = select_df.drop("creationdate")

    select_df = create_partition_columns(select_df, False)

    select_df = select_df.where(select_df.YEAR >= 2018)

    iscompleted_to_int = when(col("iscompleted") == True, 1).otherwise(0)

    select_df = select_df.withColumnRenamed("hostname", "subaccount")
    select_df = select_df.withColumnRenamed("sellerorderid", "seller_order_id")
    select_df = select_df.withColumnRenamed("ordergroup", "order_group")
    select_df = select_df.withColumnRenamed("origin", "origin_code")
    select_df = select_df.withColumnRenamed("value", "total_value")

    select_df = select_df.dropDuplicates(subset = ['id','subaccount','seller_order_id','order_group'])

    select_df = select_df.withColumn("is_completed", iscompleted_to_int)
    select_df = select_df.withColumn("country_code", col("storepreferencesdata.CountryCode"))
    select_df = select_df.withColumn("currency_code", col("storepreferencesdata.CurrencyCode"))
    select_df = select_df.withColumn("number_itens", size(col("items")))

    select_df = select_df.drop("items")
    select_df = select_df.drop("iscompleted")
    select_df = select_df.drop("storepreferencesdata")

    sum_array = UserDefinedFunction(lambda c: sum(c), IntegerType())

    select_df = select_df.withColumn("item_with_pickup", sum_array(col('shippingdata.LogisticsInfo.PickupStoreInfo.IsPickupStore').cast('array<int>')))

    select_df = select_df.drop('shippingdata')

    write_df_to_csv(select_df, s3_destination_path)
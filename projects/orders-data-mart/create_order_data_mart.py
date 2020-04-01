# !/usr/bin/env python
# coding: utf-8

import argparse
import time
import argparse 

from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_json, struct, col, size, explode, when, UserDefinedFunction, countDistinct, lit
from pyspark.sql.types import Row


spark = SparkSession \
    .builder \
    .appName('Insert orders data on data warehouse') \
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
    datetime_str_without_UTC = datetime_str.split('.')[0]
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

def count_pickup_items(df):
    order_item_has_pickup = when(col("PickupStoreInfo.IsPickupStore") == True, 1).otherwise(0)

    item_df = df \
        .select('id','subaccount','seller_order_id','order_group', explode("shippingdata.LogisticsInfo").alias("LogisticsInfo")) \
        .select('id','subaccount','seller_order_id','order_group', "LogisticsInfo.PickupStoreInfo", "LogisticsInfo.ItemIndex") \
        .withColumn("item_with_pickup", order_item_has_pickup) \
        .select('id','subaccount','seller_order_id','order_group', "item_with_pickup", "ItemIndex") \
        .groupby('id','subaccount','seller_order_id','order_group', "item_with_pickup") \
        .agg(countDistinct('ItemIndex').alias('count'))

    item_df_pickup = item_df.where(col("item_with_pickup") == 1)
    item_df_pickup = item_df_pickup.withColumnRenamed("count", "pickup_itens")
    item_df_pickup = item_df_pickup.drop("item_with_pickup")

    item_df = item_df.drop("count", "item_with_pickup")
    item_df = item_df.dropDuplicates()

    with_pickup_items = item_df.join(item_df_pickup, on=pkeys, how='left')

    with_pickup_items = with_pickup_items.na.fill(0)

    return with_pickup_items

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

    df = spark.read.parquet(s3_read_path)

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
    
    select_df = select_df.withColumn("is_completed", iscompleted_to_int)
    select_df = select_df.withColumn("country_code", col("storepreferencesdata.CountryCode"))
    select_df = select_df.withColumn("currency_code", col("storepreferencesdata.CurrencyCode"))
    select_df = select_df.withColumn("number_itens", size(col("items")))

    select_df = select_df.drop("items")
    select_df = select_df.drop("iscompleted")
    select_df = select_df.drop("storepreferencesdata")

    select_df.cache()

    with_pickup_items = count_pickup_items(select_df)

    select_df = select_df.drop('shippingdata')

    orders = select_df.join(with_pickup_items, on=pkeys, how='left')

    pickup_null_to_zero = when(col("pickup_itens").isNull(), 0).otherwise(col("pickup_itens"))
    orders = orders.withColumn("pickup_itens", pickup_null_to_zero)
    orders = orders.dropDuplicates(subset = ['id','subaccount','seller_order_id','order_group'])

    write_df_to_csv(orders, s3_destination_path)
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import desc, explode, when, col, max as f_max, sum as f_sum, count, UserDefinedFunction, lit

from datetime import datetime, timedelta
from pytz import timezone

import argparse

import json


## Creating Target DF
def get_only_recently_created_orders(df):
    '''
        Filter rows by just orders recently created
            1. Orders from Store, origin = 0 and status = order-created
            2. Orders from Marketplace, origin = 1 and status = order-accepted
    '''
    return df.filter(((df['origin'] == 1) & (df['status'] == 'order-accepted')) \
                    | ((df['origin'] == 0) & (df['status'] == 'order-created')))

def rewrite_ordergroup(df):
    '''
        Rewrite ordergroup column in MarketPlace
        The Ordergroup is null in MarketPlace, therefore rewrite to orderid column
    '''
    return df.withColumn('ordergroup', \
            when(df["origin"] == 1, df['orderid']).otherwise(df['ordergroup']))

def get_selected_columns(df):
    '''
        Get new columns for final schema
    '''
    selected_df = df.select('origin', 'orderid', 'ordergroup', 'shippingdata', \
        'storepreferencesdata.countrycode', 'storepreferencesdata.currencycode', 'hostname', \
        'value', 'totals', explode(df['shippingdata.LogisticsInfo']).alias('LogisticsInfo')) \
        .withColumnRenamed('storepreferencesdata.countrycode', 'country') \
        .withColumnRenamed('storepreferencesdata.currencycode', 'currency') \
        .withColumnRenamed('hostname', 'account').distinct()

    selected_df = selected_df.select('origin', 'ordergroup', 'orderid', 'country', \
        'LogisticsInfo.PickupStoreInfo.IsPickupStore', 'currency', 'account', \
        'value', explode(selected_df['totals']).alias('totals')) \
        .withColumnRenamed('LogisticsInfo.PickupStoreInfo.IsPickupStore', 'isPickupStore')

    return selected_df

def aggregate_pickup_store(df):
    '''
        Aggregate isPickupStore by OR
    '''
    return df.groupby('origin', 'orderid', 'ordergroup', 'country', \
            'currency', 'account', 'value', 'totals') \
            .agg(f_max('isPickupStore').alias('isPickupStore'))

def filter_shipping(df):
    '''
        Filter Totals by Shipping ID
    '''
    return df.filter(df['totals.id'] == 'Shipping') \
            .select('origin', 'orderid', 'ordergroup', 'country', 'isPickupStore', \
            'currency', 'account', 'value', col('totals.value').alias('shipping_value'))

def group_orders_by_ordergroup(df):
    '''
        Creating DF to represents orders by ordersgroup
    '''
    ordersgroup_df = df.groupby('account', 'ordergroup', 'origin', 'country', 'currency') \
        .agg(f_sum(col("value")).alias('value'), f_sum(col("shipping_value")).alias('shipping_value'), \
            f_max(col("isPickupStore")).alias('isPickupStore'))

    return ordersgroup_df.withColumn('is_free_shipping', \
            when(ordersgroup_df["shipping_value"] == 0, True).otherwise(False))

def group_orders_by_account(df):
    '''
        Creating orders DF grouped by account, country, currency and origin
    '''
    return df.groupBy('origin', 'country', 'currency', 'account') \
            .agg(count('ordergroup').alias('orders_number'), f_sum(col("is_free_shipping").cast("long")).alias('orders_free_shipping'), \
            f_sum(col("isPickupStore").cast("long")).alias('orders_pickup'), f_sum('value').alias('revenue'))

def create_target_df(df):
    orders_recently_created_df = get_only_recently_created_orders(df)
    rewrite_ordergroup_df = rewrite_ordergroup(orders_recently_created_df)
    selected_df = get_selected_columns(rewrite_ordergroup_df)
    agg_pickup_store_df = aggregate_pickup_store(selected_df)
    filtered_shipping_df = filter_shipping(agg_pickup_store_df)
    ordersgroup_df = group_orders_by_ordergroup(filtered_shipping_df)
    account_df = group_orders_by_account(ordersgroup_df)

    return account_df


## Functions to partition data
def create_partition_columns(df, last_hour_date):
    return df.withColumn('ingestion_year', lit(last_hour_date.year)) \
            .withColumn('ingestion_month', lit(last_hour_date.month)) \
            .withColumn('ingestion_day', lit(last_hour_date.day)) \
            .withColumn('ingestion_hour', lit(last_hour_date.hour))

def get_last_hour_date(timezone_str='UTC'):
    class MTime(object):
        def __init__(self, y_str, m_str, d_str, h_str, min_str, s_srt):
            self.year = y_str
            self.month = m_str
            self.day = d_str
            self.hour = h_str
            self.min = min_str
            self.second = s_srt
    
    now = datetime.now(timezone(timezone_str))
    last_hour_date = now - timedelta(hours=1)
    year, month, day = str(last_hour_date).split(' ')[0].split('-')
    hour, minn, second, _ = str(last_hour_date).split(' ')[1].split(':')
    last_hour_date = MTime(year, month, day, hour, minn, second)
    
    return last_hour_date

## Main
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

def main():
    ## Start Spark Session
    spark = SparkSession \
        .builder \
        .appName('Creating Target Data Orders')\
        .getOrCreate()

    s3_read_path, s3_destination_path = _read_args()
    last_hour_date = get_last_hour_date()

    df = spark.read \
        .parquet(s3_read_path) \
        .distinct()

    target_df = create_target_df(df)
    target_df = create_partition_columns(target_df, last_hour_date)

    target_df.repartition('ingestion_year','ingestion_month','ingestion_day', 'ingestion_hour') \
            .write \
            .partitionBy('ingestion_year','ingestion_month','ingestion_day', 'ingestion_hour') \
            .mode('append') \
            .parquet(s3_destination_path)

main()
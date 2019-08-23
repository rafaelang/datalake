# coding: utf-8

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import BooleanType, DateType
import pyspark.sql.functions as f

import json
import time
from datetime import datetime, timedelta
from pytz import timezone
import sys, os, argparse


global LOSS_DATETIME_UTF
LOSS_DATETIME_UTF = datetime.strptime('2019-08-23 17:00', '%Y-%m-%d %H:%M')


### Creating partitions: Day, Month and Year

def getYear(lastChange, creationDate):
    date = lastChange if lastChange is not None else creationDate
    return date.split('T')[0].split('-')[0]

def getMonth(lastChange, creationDate):
    date = lastChange if lastChange is not None else creationDate
    return date.split('T')[0].split('-')[1]

def getDay(lastChange, creationDate):
    date = lastChange if lastChange is not None else creationDate
    return date.split('T')[0].split('-')[2]

def create_partition_columns(df):
    """Create the Columns for the Partitions"""

    # Register functions as Spark UDFs 
    udf_getYear = UserDefinedFunction(getYear, StringType())
    udf_getMonth = UserDefinedFunction(getMonth, StringType())
    udf_getDay = UserDefinedFunction(getDay, StringType())

    df = df.withColumn('YEAR', udf_getYear(df.LastChange, df.CreationDate))
    df = df.withColumn('MONTH', udf_getMonth(df.LastChange, df.CreationDate))
    df = df.withColumn('DAY', udf_getDay(df.LastChange, df.CreationDate))

    return df

def create_datetime_obj(creation_date, last_change):
    date = last_change if last_change is not None else creation_date
    return datetime.strptime(date[:-2], '%Y-%m-%dT%H:%M:%S.%f')


def created_before(creation_date):
    return creation_date < LOSS_DATETIME_UTF


def main():
    datasrc_s3 = "s3://vtex.datalake/structured_json/checkout/00_CheckoutOrder/*/id/"
    destination_path = "s3://vtex.datalake/consumable_tables/checkout/"
    
    df = spark.read.json(datasrc_s3)
        
    udf_created_before = UserDefinedFunction(created_before, BooleanType())
    udf_create_datetime_obj = UserDefinedFunction(create_datetime_obj, DateType())
    
    df = df.withColumn('CreationDateObj', udf_create_datetime_obj(df.CreationDate, df.LastChange))

    df = df.withColumn('WasCreatedBeforeLost', udf_created_before(df.CreationDateObj))

    for r in df.filter((f.col('WasCreatedBeforeLost') == False)).take(10): 
        print (r.WasCreatedBeforeLost, r.CreationDate)

    df = df.filter((f.col('WasCreatedBeforeLost')))
    
    df = create_partition_columns(df)
        
    #### Save table to S3 using Parquet format and partitioning by defined columns
    df \
        .repartition('YEAR','MONTH','DAY') \
        .write \
        .partitionBy(['YEAR','MONTH','DAY']) \
        .mode('append') \
        .parquet(destination_path)

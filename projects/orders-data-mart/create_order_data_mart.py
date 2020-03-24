# !/usr/bin/env python
# coding: utf-8

import boto3
import base64
from botocore.exceptions import ClientError

from datetime import datetime
import time

rds_client = boto3.client('rds-data')

database_name = "data_mart_brand"
db_cluster_arn = "arn:aws:rds:us-east-2:282989224251:cluster:brand-test-2-serverless"
db_credentials_secrets_store_arn = ""

def execute_statement(sql, sql_parameters=[]):
    response = rds_client.execute_statement(
        secretArn=db_credentials_secrets_store_arn,
        database=database_name,
        resourceArn=db_cluster_arn,
        sql=sql,
        parameters=sql_parameters
    )
    return response

def batch_execute_statement(sql, sql_parameter_sets):
    response = rds_client.batch_execute_statement(
        secretArn=db_credentials_secrets_store_arn,
        database=database_name,
        resourceArn=db_cluster_arn,
        sql=sql,
        parameterSets=sql_parameter_sets
    )
    return response

sql = """
        CREATE TABLE IF NOT EXISTS orders (
            id VARCHAR(50) NOT NULL,
            order_group VARCHAR(50) NOT NULL,
            subaccount VARCHAR(50) NOT NULL,
            seller_order_id VARCHAR(50),
            origin_code INTEGER,
            number_itens INTEGER,
            total_value DOUBLE,
            creation_date DATETIME,
            pickup_itens INTEGER,
            country_code VARCHAR(3),
            currency_code VARCHAR(3),
            is_completed BOOLEAN,
            status VARCHAR(50),
            PRIMARY KEY (id, order_group, subaccount, seller_order_id)
        )
    """

response = execute_statement(sql)

sql = """
        INSERT INTO orders 
            (id, order_group, subaccount, seller_order_id, 
                origin_code, number_itens, total_value, 
                creation_date, pickup_itens, country_code, 
                currency_code, is_completed, status) 
        VALUES 
            (:id, :order_group, :subaccount, :seller_order_id, 
                :origin_code, :number_itens, :total_value, 
                :creation_date, :pickup_itens, :country_code, 
                :currency_code, :is_completed, :status) 
        ON DUPLICATE KEY UPDATE 
            is_completed = :is_completed,
            status = :status, 
            number_itens = :number_itens,
            total_value = :total_value,
            pickup_itens = :pickup_itens
    """

def get_sql_params(**kwargs):
    mid = kwargs and kwargs['id']
    order_group = kwargs and kwargs['order_group']
    subaccount = kwargs and kwargs['subaccount']
    seller_order_id = kwargs and kwargs['seller_order_id']
    origin_code = kwargs and kwargs['origin_code']
    number_itens = kwargs and kwargs['number_itens']
    total_value = kwargs and kwargs['total_value']
    creation_date = kwargs and kwargs['creation_date']
    pickup_itens = kwargs and kwargs['pickup_itens']
    seller_order_id = kwargs and kwargs['seller_order_id']
    country_code = kwargs and kwargs['country_code']
    currency_code = kwargs and kwargs['currency_code']
    is_completed = kwargs and kwargs['is_completed']
    status = kwargs and kwargs['status']
    
    return [
        {
            'name':'id', 
            'value':{'stringValue': mid}
        },
        {
            'name':'order_group', 
            'value':{'stringValue': order_group}
        },
        {
            'name':'subaccount', 
            'value':{'stringValue': subaccount}
        },
        {
            'name':'seller_order_id', 
            'value':{'stringValue': seller_order_id}
        },
        {
            'name':'origin_code', 
            'value':{'stringValue': origin_code}
        },
        {
            'name':'number_itens', 
            'value':{'stringValue': number_itens}
        },
        {
            'name':'total_value', 
            'value':{'stringValue': total_value}
        },
        {
            'name':'creation_date', 
            'value':{'stringValue': creation_date}
        },
        {
            'name':'pickup_itens', 
            'value':{'stringValue': pickup_itens}
        },
        {
            'name':'country_code', 
            'value':{'stringValue': country_code}
        },
        {
            'name':'currency_code', 
            'value':{'stringValue': currency_code}
        },
        {
            'name':'is_completed', 
            'value':{'stringValue': is_completed}
        },
        {
            'name':'status', 
            'value':{'stringValue': status}
        } 
    ]


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_json, struct, col, size, explode, when, UserDefinedFunction
from pyspark.sql.types import Row

spark = SparkSession.builder.appName('Insert orders data on data warehouse').getOrCreate()


READ_PATH = "s3://vtex.datalake/raw_data/orders/parquet_stage/ingestion_year=2020/ingestion_month=02/ingestion_day=01/ingestion_hour=00/DataStreamCleansedOrders-2-2020-02-01-00-00-03-d6aaef81-6a9b-421c-b195-c67b1a088660.parquet"
df = spark.read.parquet(READ_PATH)


cols = [
    'id','hostname','status', 'shippingdata',
    'items','sellerorderid','ordergroup', 'creationdate',
    'origin','value','storepreferencesdata', 'iscompleted'
]
select_df = df.select(*cols)

select_df = select_df.withColumnRenamed("hostname", "subaccount")
select_df = select_df.withColumnRenamed("sellerorderid", "seller_order_id")
select_df = select_df.withColumnRenamed("ordergroup", "order_group")
select_df = select_df.withColumnRenamed("origin", "origin_code")
select_df = select_df.withColumnRenamed("value", "total_value")

iscompleted_to_int = when(col("iscompleted") == True, 1).otherwise(0)
select_df = select_df.withColumn("is_completed", iscompleted_to_int)
select_df = select_df.withColumn("country_code", col("storepreferencesdata.CountryCode"))
select_df = select_df.withColumn("currency_code", col("storepreferencesdata.CurrencyCode"))
select_df = select_df.withColumn("number_itens", size(col("items")))

select_df = select_df.drop("items")
select_df = select_df.drop("storepreferencesdata")

def format_datetime_str(datetime_str):
    datetime_pattern = '%Y-%m-%dT%H:%M:%S.%f'
    datetime_str_without_UTC = datetime_str[:-len('XZ')]
    datetime_object = datetime.strptime(datetime_str_without_UTC, datetime_pattern)
    return str(datetime_object.replace(microsecond=0))

format_datetime_str_UDF = UserDefinedFunction(format_datetime_str, StringType())
select_df = select_df.withColumn('creation_date', format_datetime_str_UDF(select_df.creationdate))

pkeys = [
    'id','subaccount','seller_order_id','order_group'
]

order_item_has_pickup = when(col("PickupStoreInfo.IsPickupStore") == True, 1).otherwise(0)

itmd_df = select_df \
    .select('id','subaccount','seller_order_id','order_group', explode("shippingdata.LogisticsInfo").alias("LogisticsInfo")) \
    .select('id','subaccount','seller_order_id','order_group', "LogisticsInfo.PickupStoreInfo", "LogisticsInfo.ItemIndex") \
    .withColumn("item_with_pickup", order_item_has_pickup) \
    .select('id','subaccount','seller_order_id','order_group', "item_with_pickup", "ItemIndex") \
    .groupby('id','subaccount','seller_order_id','order_group', "item_with_pickup") \
    .count()

itmd_df_pickup = itmd_df.where(col("item_with_pickup") == 1)
itmd_df_pickup = itmd_df_pickup.withColumnRenamed("count", "pickup_itens")
itmd_df_pickup = itmd_df_pickup.drop("item_with_pickup")

itmd_df = itmd_df.drop("count", "item_with_pickup")

joined = itmd_df.join(itmd_df_pickup, on=pkeys, how='left')

joined = joined.na.fill(0)

select_df = select_df.drop('shippingdata')

orders = select_df.join(joined, on=pkeys, how='left')

pickup_null_to_zero = when(col("pickup_itens").isNull(), 0).otherwise(col("pickup_itens"))
orders = orders.withColumn("pickup_itens", pickup_null_to_zero)

# >>>
i = time.time()
orders_list = orders.rdd \
    .map(lambda row: 
         {'id': row.id, 
          'subaccount': row.subaccount, 
          'seller_order_id': row.seller_order_id, 
          'order_group': row.order_group, 
          'pickup_itens': row.pickup_itens, 
          'status': row.status, 
          'creation_date': row.creation_date, 
          'origin_code': row.origin_code,
          'total_value': row.total_value,
          'is_completed': row.is_completed,
          'country_code': row.country_code,
          'currency_code': row.currency_code,
          'number_itens': row.number_itens}) \
    .collect()
e = time.time()
print("Transforming df in list", str(e - i))

sql_parameters_set = [get_sql_params(**row) for row in orders_list]

# >>>
i = time.time()
for i in range(0, len(sql_parameters_set), 100):
    if i+100 > len(sql_parameters_set):
        response = batch_execute_statement(sql, sql_parameters_set[i:])
    else:
        response = batch_execute_statement(sql, sql_parameters_set[i:i+100])
e = time.time()
print("sending batch sql statements in buffers of 100 orders", str(e - i))
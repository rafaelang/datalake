from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import UserDefinedFunction, to_json, struct
from pyspark.sql.types import *

import argparse

import json

## Building SparkSession

spark = SparkSession \
    .builder \
    .appName('Creating Stage Data Orders')\
    .getOrCreate()

## Creating Struct Constants

MARKETPLACE_STRUCT = StructType([StructField("IsCertified",BooleanType(),True),StructField("Name",StringType(),True)])
TOTALS_STRUCT = ArrayType(StructType([StructField("Id", StringType(), True), StructField("Name", StringType(), True), StructField("value", LongType(), True)]), True)
SHIPPING_DATA_STRUCT = StructType([StructField("Address",StructType([StructField("AddressId",StringType(),True),StructField("AddressType",StringType(),True),StructField("City",StringType(),True),StructField("Complement",StringType(),True),StructField("Country",StringType(),True),StructField("GeoCoordinates",ArrayType(DoubleType(),True),True),StructField("Neighborhood",StringType(),True),StructField("Number",StringType(),True),StructField("PostalCode",StringType(),True),StructField("Reference",StringType(),True),StructField("State",StringType(),True),StructField("Street",StringType(),True)]),True),StructField("LogisticsInfo",ArrayType(StructType([StructField("AddressId",StringType(),True),StructField("DeliveryChannel",StringType(),True),StructField("DeliveryChannels",ArrayType(StructType([StructField("Id",StringType(),True)]),True),True),StructField("DeliveryCompany",StringType(),True),StructField("DeliveryIds",ArrayType(StructType([StructField("CourierId",StringType(),True),StructField("CourierName",StringType(),True),StructField("DockId",StringType(),True),StructField("Quantity",LongType(),True),StructField("TotalListPrice",StringType(),True),StructField("WarehouseId",StringType(),True)]),True),True),StructField("DeliveryWindow",StructType([StructField("EndDateUtc",StringType(),True),StructField("StartDateUtc",StringType(),True),StructField("listPrice",LongType(),True),StructField("price",LongType(),True),StructField("tax",LongType(),True)]),True),StructField("ItemId",StringType(),True),StructField("ItemIndex",LongType(),True),StructField("PickupStoreInfo",StructType([StructField("AdditionalInfo",StringType(),True),StructField("Address",StructType([StructField("AddressId",StringType(),True),StructField("AddressType",StringType(),True),StructField("City",StringType(),True),StructField("Complement",StringType(),True),StructField("Country",StringType(),True),StructField("GeoCoordinates",ArrayType(DoubleType(),True),True),StructField("Neighborhood",StringType(),True),StructField("Number",StringType(),True),StructField("PostalCode",StringType(),True),StructField("ReceiverName",StringType(),True),StructField("Reference",StringType(),True),StructField("State",StringType(),True),StructField("Street",StringType(),True)]),True),StructField("DockId",StringType(),True),StructField("FriendlyName",StringType(),True),StructField("IsPickupStore",BooleanType(),True)]),True),StructField("PolygonName",StringType(),True),StructField("SelectedDeliveryChannel",StringType(),True),StructField("ShippingEstimate",StringType(),True),StructField("ShippingEstimateDate",StringType(),True),StructField("ShipsTo",ArrayType(StringType(),True),True),StructField("listPrice",LongType(),True),StructField("lockTTL",StringType(),True),StructField("price",LongType(),True),StructField("selectedSla",StringType(),True),StructField("sellingPrice",LongType(),True),StructField("slas",ArrayType(StructType([StructField("AvailableDeliveryWindows",ArrayType(StructType([StructField("EndDateUtc",StringType(),True),StructField("StartDateUtc",StringType(),True),StructField("listPrice",StringType(),True),StructField("price",LongType(),True),StructField("tax",LongType(),True)]),True),True),StructField("DeliveryChannel",StringType(),True),StructField("DeliveryIds",ArrayType(StructType([StructField("CourierId",StringType(),True),StructField("CourierName",StringType(),True),StructField("DockId",StringType(),True),StructField("Quantity",LongType(),True),StructField("TotalListPrice",StringType(),True),StructField("WarehouseId",StringType(),True)]),True),True),StructField("DeliveryWindow",StructType([StructField("EndDateUtc",StringType(),True),StructField("StartDateUtc",StringType(),True),StructField("listPrice",StringType(),True),StructField("price",LongType(),True),StructField("tax",LongType(),True)]),True),StructField("Id",StringType(),True),StructField("LockTTL",StringType(),True),StructField("Name",StringType(),True),StructField("PickupDistance",DoubleType(),True),StructField("PickupPointId",StringType(),True),StructField("PickupStoreInfo",StructType([StructField("AdditionalInfo",StringType(),True),StructField("Address",StructType([StructField("AddressId",StringType(),True),StructField("AddressType",StringType(),True),StructField("City",StringType(),True),StructField("Complement",StringType(),True),StructField("Country",StringType(),True),StructField("GeoCoordinates",ArrayType(DoubleType(),True),True),StructField("Neighborhood",StringType(),True),StructField("Number",StringType(),True),StructField("PostalCode",StringType(),True),StructField("ReceiverName",StringType(),True),StructField("Reference",StringType(),True),StructField("State",StringType(),True),StructField("Street",StringType(),True)]),True),StructField("DockId",StringType(),True),StructField("FriendlyName",StringType(),True),StructField("IsPickupStore",BooleanType(),True)]),True),StructField("PolygonName",StringType(),True),StructField("ShippingEstimate",StringType(),True),StructField("ShippingEstimateDate",StringType(),True),StructField("listPrice",LongType(),True),StructField("price",LongType(),True),StructField("tax",LongType(),True)]),True),True)]),True),True)])

## Functions to delete irrelevant columns in Data Frame

def delete_irrelevant_columns(df):
    '''
        Deleting irrelevant columns to later analysis in target data.
    '''
    # Columns evaluated as irrelevant
    irrelevant_columns = ['LastMessage', 'ClientProfileData', 'GiftRegistryData', 'EmailTracked', 'CallCenterOperatorData',
                          'RowundingError', 'OpenTextField', 'FollowUpEmail', 'PaymentData', 'ItemMetadata', 'merchantname',
                          'CreationEnvironment', 'CommercialConditionData', 'MarketplaceServicesEndpoint', 'ReceiptData',
                          'ApprovedBy', 'CancelledBy', 'CanceledBy', 'IsCheckedIn', 'Sequence']

    return df.drop(*irrelevant_columns)

def delete_irrelevant_columns_in_marketplace(data):
    '''
        Deleting irrelevant nested columns in marketplace.
    '''
    data = data and json.loads(data)
    
    marketplace = None
    if(data and "marketplace" in data):
        marketplace = data["marketplace"]
        if("BaseURL" in marketplace):
            del marketplace["BaseURL"]
    
    return marketplace

def delete_irrelevant_columns_in_totals(data):
    '''
        Deleting irrelevant nested columns in totals.
    '''
    data = data and json.loads(data)
    
    totals = None
    if(data and "totals" in data):
        totals = data["totals"]
        for total in totals:
            if("AlternativeTotals" in total):
                del total["AlternativeTotals"]
    
    return totals

def delete_irrelevant_columns_in_shipping_data(data):
    '''
        Deleting irrelevant nested columns in shiping data.
    '''
    data = data and json.loads(data)
    
    new_shipping_data = {}
    if(data and "shippingdata" in data):
        shipping_data = data["shippingdata"]
        new_shipping_data["Address"] = shipping_data["Address"]
        
        if("ReceiverName" in new_shipping_data["Address"]):
            del new_shipping_data["Address"]["ReceiverName"]
        
        new_shipping_data["LogisticsInfo"] = shipping_data["LogisticsInfo"]
    
    return new_shipping_data

def create_stage_data(df):
    '''
        Create stage data in Data Frame, ie no relevant data.
    '''
    ## Register functions as Spark UDF
    udf_delete_irrelevant_columns_in_marketplace = UserDefinedFunction(delete_irrelevant_columns_in_marketplace, MARKETPLACE_STRUCT)
    udf_delete_irrelevant_columns_in_totals = UserDefinedFunction(delete_irrelevant_columns_in_totals, TOTALS_STRUCT)
    udf_delete_irrelevant_columns_in_shipping_data = UserDefinedFunction(delete_irrelevant_columns_in_shipping_data, SHIPPING_DATA_STRUCT)

    ## Deleting irrelevants columns
    df_without_irrelevant_columns = delete_irrelevant_columns(df)
    
    ## Generating new column with JSON values
    df_without_irrelevant_columns = df_without_irrelevant_columns.withColumn("ToJSON", to_json(struct([df_without_irrelevant_columns[x] for x in df_without_irrelevant_columns.columns])))
    
    ## Cleansed nested columns
    cleansed_df = df_without_irrelevant_columns.withColumn("marketplace", udf_delete_irrelevant_columns_in_marketplace("ToJSON")) \
        .withColumn("totals", udf_delete_irrelevant_columns_in_totals("ToJSON")) \
        .withColumn("shippingdata", udf_delete_irrelevant_columns_in_shipping_data("ToJSON")) \
        .drop("ToJSON")
    
    return cleansed_df

## Functions to partition data

def getYear(creationDate):
    return creationDate.split('T')[0].split('-')[0]

def getMonth(creationDate):
    return creationDate.split('T')[0].split('-')[1]

def getDay(creationDate):
    return creationDate.split('T')[0].split('-')[2]

def getHour(creationDate):
    return creationDate.split('T')[1].split(':')[0]

def create_partition_columns(df):
    #### Register functions as Spark UDFs 
    udf_getYear = UserDefinedFunction(getYear, StringType())
    udf_getMonth = UserDefinedFunction(getMonth, StringType())
    udf_getDay = UserDefinedFunction(getDay, StringType())
    udf_getHour = UserDefinedFunction(getHour, StringType())

    df = df.withColumn('ingestion_year', udf_getYear(df.creationdate)) \
           .withColumn('ingestion_month', udf_getMonth(df.creationdate)) \
           .withColumn('ingestion_day', udf_getDay(df.creationdate)) \
           .withColumn('ingestion_hour', udf_getHour(df.creationdate))

    return df

## Reading args to get read and destination path.

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

## Run PySpark

def main():
    s3_read_path, s3_destination_path = _read_args()
    
    df = spark.read.parquet("s3://" + s3_read_path)
    df = create_stage_data(df)
    df = create_partition_columns(df)
    
    df.repartition('ingestion_year','ingestion_month','ingestion_day', 'ingestion_hour') \
        .write \
        .partitionBy('ingestion_year','ingestion_month','ingestion_day', 'ingestion_hour') \
        .mode('append') \
        .parquet('s3://' + s3_destination_path)

main()
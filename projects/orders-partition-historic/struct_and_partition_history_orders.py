# conding: utf-8
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import UserDefinedFunction, col
from pyspark.sql.types import *

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_json, struct
from pyspark.sql.types import Row

import boto3

import argparse
import json


spark = SparkSession \
    .builder \
    .appName('Transform Historic Orders')\
    .getOrCreate()

#### Get Schema Type from data frame
def get_schema_type(name_column, df):
    types = filter(lambda f: f.name == name_column, df.schema.fields)
    if len(types) > 0:
        type_index = 0
        itemType = types[type_index].dataType
        return itemType
    else:
        None


### Returns a boolean indicating if exists column in dataframe.
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False
    

### Removing problematic keys in RateAndBenefits
def convert_rates_and_benefits(data):
    data = data and json.loads(data)
    if "RatesAndBenefitsData" in data:
        data = data and data["RatesAndBenefitsData"]
        KEY_IDENTIFIERS = "RateAndBenefitsIdentifiers"
        KEY_MATCH_PARAMS = "MatchedParameters"
        KEY_ADDINFO = "AdditionalInfo"
        if data and KEY_IDENTIFIERS in data:
            len_key_identifiers = len(data[KEY_IDENTIFIERS]) if data[KEY_IDENTIFIERS] else 0
            for i in range(len_key_identifiers):
                if KEY_MATCH_PARAMS in data[KEY_IDENTIFIERS][i]:
                    del data[KEY_IDENTIFIERS][i][KEY_MATCH_PARAMS]
                if KEY_ADDINFO in data[KEY_IDENTIFIERS][i]:
                    del data[KEY_IDENTIFIERS][i][KEY_ADDINFO]
        
        new_row = Row(**data) 
        
        return data


def clean_payment_data(pay_data):
    KEY_TRANSACTIONS = "Transactions"
    KEY_CONN_RESP = "connectorResponses"
    KEY_PAYMENTS = "Payments"
    
    pay_data = json.loads(pay_data)
    if pay_data and KEY_TRANSACTIONS in pay_data:
        for i in range(len(pay_data[KEY_TRANSACTIONS])):
            if KEY_PAYMENTS in pay_data[KEY_TRANSACTIONS][i]:
                payments = pay_data[KEY_TRANSACTIONS][i][KEY_PAYMENTS]
                for j in range(len(payments)):
                    if KEY_CONN_RESP in payments[j]:
                        del pay_data[KEY_TRANSACTIONS][i][KEY_PAYMENTS][j][KEY_CONN_RESP]

    return Row(**pay_data)
    
    
#### Remove keys with Attachment content, because it isn't necessary for data analyse. 
def remove_attachments(df, cleansed_df):
    ATTACHMENT = "attachment"
    
    def _remove_attachments_key(dic):
        dic_copy = dic.copy()
        for key in dic_copy:
            if(ATTACHMENT in key.lower()):
                del dic[key]
            elif(type(dic_copy[key]) == dict):
                _remove_attachments_key(dic[key])
            elif(type(dic_copy[key]) == list):
                for item in dic_copy[key]:
                    if(type(item) == dict):
                        _remove_attachments_key(item)
        return dic
        
    def _field_cleansing(row, field_name):
        row = row and json.loads(row)
        if row:
            field = row.get(field_name, {})
            if type(field) is dict:
                field = _remove_attachments_key(field)
            return field

    for field in df.schema.fields:
        field_type = field.dataType
        field_name = field.name
        if ATTACHMENT in field_name.lower():
            df = df.drop(field_name)
        elif field_type != StringType():
            print('Getting schea type for field {}'.format(field_name))
            structured_field_type = get_schema_type(field_name, cleansed_df)
            if structured_field_type:
                print('Everything was OK to {}'.format(field_name))
                udf_attachments_cleansing = UserDefinedFunction(lambda row: _field_cleansing(row, field_name), structured_field_type)
                df = df.withColumn(field_name, udf_attachments_cleansing("ToJSON"))
            
    return df

def clean_item_metadata(item_metadata):
    '''
        Remove malformed inner fields from json object (assemblyOptions).

        Args:
            item_metadata (dict): order item's metadata.
        Returns:
            the input argument but with structure changes inner fields.         
    '''
    item_metadata = json.loads(item_metadata)
    
    len_items_item_metadata = len(item_metadata["Items"])
    for i in range(len_items_item_metadata):
        if ("AssemblyOptions" in item_metadata["Items"][i]):
            del item_metadata["Items"][i]["AssemblyOptions"]
    
    return Row(**item_metadata)

### Creating partitions: Day, Month and Year
def getYear(creationDate):
    return creationDate.split('T')[0].split('-')[0]

def getMonth(creationDate):
    return creationDate.split('T')[0].split('-')[1]

def getDay(creationDate):
    return creationDate.split('T')[0].split('-')[2]

def getHour(creationDate):
    return creationDate.split('T')[1].split(':')[0]

### Create the Columns for the Partitions
def create_partition_columns(df):
    #### Register functions as Spark UDFs 
    udf_getYear = UserDefinedFunction(getYear, StringType())
    udf_getMonth = UserDefinedFunction(getMonth, StringType())
    udf_getDay = UserDefinedFunction(getDay, StringType())
    udf_getHour = UserDefinedFunction(getHour, StringType())    

    df = df.withColumn('ingestion_year', udf_getYear(df.CreationDate))
    df = df.withColumn('ingestion_month', udf_getMonth(df.CreationDate))
    df = df.withColumn('ingestion_day', udf_getDay(df.CreationDate))
    df = df.withColumn('ingestion_hour', udf_getHour(df.CreationDate))

    return df

## Converting data with spark
def struct_data_frame(df, cleansed_df):
    # Create UDFs
    udf_convert_rates_and_benefits = UserDefinedFunction(convert_rates_and_benefits, get_schema_type("RatesAndBenefitsData", cleansed_df))
    udf_clean_item_metadata = UserDefinedFunction(clean_item_metadata, get_schema_type("ItemMetadata", cleansed_df))
    udf_clean_payment_data = UserDefinedFunction(clean_payment_data, get_schema_type("PaymentData", cleansed_df))

    df = df.withColumn("RatesAndBenefitsData", udf_convert_rates_and_benefits("ToJSON")) if has_column(df, 'RatesAndBenefitsData') else df
    df = df.withColumn("ItemMetadata", udf_clean_item_metadata("ToJSON")) if has_column(df, "ItemMetadata") else df
    df = df.withColumn("PaymentData", udf_clean_payment_data("ToJSON")) if has_column(df, "PaymentData") else df
    
    # Columns to droping
    df = df.drop("CustomData")

    df = remove_attachments(df, cleansed_df)

    return df

def _read_args():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--folder-prefix-filter',
        help='Prefix folder to be read. For example: 00000',
        required=True
    )
    parser.add_argument(
        '--destination-path', 
        help='Where the data to be written. For example: consumable_tables/checkout',
        required=True
    )
    args=parser.parse_args()
    
    return args.folder_prefix_filter, args.destination_path

## Get all paths in bucket from prefix.
def get_all_paths(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objs = bucket.objects.filter(Prefix=prefix)
    paths = list(map(lambda o: "s3://" + o.bucket_name + "/" + o.key, objs))
    
    return paths

## Rewrites the column names in data frame to lower case, because it's necessary
## to keep the Firehose's pattern that write column names in lower case too.
def rewriteColumnNames(df):
    renamed_columns = { column: column.lower() for column in df.columns }
    renamed_columns_df = df.select([col(c).alias(renamed_columns.get(c, c)) for c in df.columns])

    return renamed_columns_df

if __name__ == "__main__":
    folder_prefix_filter, destination_path = _read_args()
    
    ## Getting Schema's Interface from Checkout Structured Json
    cleansed_df = spark.read.json(get_all_paths('vtex.datalake', 'sample_schema/orders'))

    ## Reading data from Checkout History
    df = spark.read.json(get_all_paths('vtex-analytics-import', 'vtex-orders-index/'+folder_prefix_filter))

    ## Temporary column to convert df data to JSON. 
    df = df.withColumn("ToJSON", to_json(struct([df[x] for x in df.columns])))    
    
    df = struct_data_frame(df, cleansed_df)
    df = create_partition_columns(df)

    ## Deletes the temporary column ToJSON
    df = df.drop("ToJSON")

    df = rewriteColumnNames(df)

    ### Writing data into S3 bucket
    #### Save table to S3 using Parquet format and partitioning by defined columns
    df.repartition('ingestion_year','ingestion_month','ingestion_day', 'ingestion_hour')\
        .write\
        .partitionBy('ingestion_year','ingestion_month','ingestion_day', 'ingestion_hour')\
        .mode('append')\
        .parquet('s3://vtex.datalake/' + destination_path + '/')        
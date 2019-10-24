# conding: utf-8
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_json, struct
from pyspark.sql.types import Row

import argparse
import json


spark = SparkSession \
    .builder \
    .appName('Transform Checkout')\
    .getOrCreate()

#### Get Schema Type from data frame
def get_schema_type(name_column, structured_df):
    types = filter(lambda f: f.name == name_column, structured_df.schema.fields)
    type_index = 0
    itemType = types[type_index].dataType

    return itemType


### Returns a boolean indicating if exists column in dataframe.
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False
    

#### Casting Items
def convert_item(data):
    def _parse_product_categories(raw_product_categories):
        product_categories = []
        for k,v in raw_product_categories.items():
            product_categories.append({'id': k,'name': v})
        return product_categories    
    
    data = json.loads(data)
    if "Items" in data:
        items_rows = []
        items = data["Items"]        
        for item in items:
            product_categories = _parse_product_categories(item.get('productCategories', {}))
            item['productCategories'] = product_categories
            new_row = Row(**item)
            items_rows.append(new_row)
        return items_rows
   

#### Casting RateAndBenefits
def convert_ratesandbenefits(data):
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

    
def paymentdata_cleansing(data):
    data = json.loads(data)
    if "PaymentData" in data:
        if data and "Transactions" in data:
            del data["Transactions"]
        new_row = Row(**data)
        return data
    
    
#### Remove keys with Attachment content, buecause it is not necessary for data analyse. 
def remove_attachments(df, structured_df):
    
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
            structured_field_type = get_schema_type(field_name, structured_df)
            udf_attachments_cleansing = UserDefinedFunction(lambda row: _field_cleansing(row, field_name), structured_field_type)
            df = df.withColumn(field_name, udf_attachments_cleansing("ToJSON"))
            
    return df


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

### Create the Columns for the Partitions
def create_partition_columns(df):
    #### Register functions as Spark UDFs 
    udf_getYear = UserDefinedFunction(getYear, StringType())
    udf_getMonth = UserDefinedFunction(getMonth, StringType())
    udf_getDay = UserDefinedFunction(getDay, StringType())

    df = df.withColumn('YEAR', udf_getYear(df.LastChange, df.CreationDate))
    df = df.withColumn('MONTH', udf_getMonth(df.LastChange, df.CreationDate))
    df = df.withColumn('DAY', udf_getDay(df.LastChange, df.CreationDate))

    return df

### Converting data with spark
def struct_data_frame(df, structured_df):
    # Create UDFs
    udf_getData = UserDefinedFunction(convert_item, get_schema_type("Items", structured_df))
    udf_convert_ratesandbenefits = UserDefinedFunction(convert_ratesandbenefits, get_schema_type("RatesAndBenefitsData", structured_df))
    udf_paymentdata_cleansing = UserDefinedFunction(paymentdata_cleansing, get_schema_type("PaymentData", structured_df))
      
    df = df.withColumn('Items', udf_getData("ToJSON")) if has_column(df, 'Items') else df
    df = df.withColumn("RatesAndBenefitsData", udf_convert_ratesandbenefits("ToJSON")) if has_column(df, 'RatesAndBenefitsData') else df
    df = df.withColumn("PaymentData", udf_paymentdata_cleansing("ToJSON")) if has_column(df, "PaymentData") else df
    
    df = df.drop("ItemMetadata")
    df = df.drop("CustomData")
    df = df.drop("CommercialConditionData")
    df = remove_attachments(df, structured_df)

    return df

def _read_args():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--folder-prefix-filter', 
        help='', # TODO
        required=True,
    )
    parser.add_argument(
        '--destination-path', 
        help='Where the data to be written. For example: consumable_tables/checkout',
        required=False,
        default=''
    )
    args=parser.parse_args()
    
    return args.folder_prefix_filter, args.destination_path
    
if __name__ == "__main__":
    folder_prefix_filter, destination_path = _read_args()
    
    ### Getting Schema's Interface from Checkout Structured Json
    structured_jsons_path = 's3://vtex.datalake/structured_json/' + folder_prefix_filter + '*/*/*'
    structured_df = spark.read.json(structured_jsons_path)

    ### Reading data from Checkout History
    data_path = 's3://vtex-analytics-import/vtex-orders-index/' + folder_prefix_filter + '*/*'
    df = spark.read.json(data_path)    

    df = df.withColumn("ToJSON", to_json(struct([df[x] for x in df.columns])))    
    
    df = struct_data_frame(df, structured_df)
    df = create_partition_columns(df)
    
    df = df.drop("ToJSON")

    ### Writing data into S3 bucket
    #### Save table to S3 using Parquet format and partitioning by defined columns
    df.repartition('YEAR','MONTH','DAY')\
        .write\
        .partitionBy('YEAR','MONTH','DAY')\
        .mode('append')\
        .parquet('s3://vtex.datalake/' + destination_path + '/')        
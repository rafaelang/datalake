# conding: utf-8
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *

from pyspark.sql.utils import AnalysisException

import json

#### Start Spark session
sc = SparkContext('local')
spark = SparkSession(sc)

### Getting Schema's Interface from Checkout Structured Json
structured_jsons_path = 's3://vtex.datalake/structured_json/checkout/00_CheckoutOrder/0{*}/id/*'
structured_df = spark.read.json(structured_jsons_path)

#### Get Schema Type from data frame
def get_schema_type(name_column):
    types = filter(lambda f: f.name == name_column, structured_df.schema.fields)
    type_index = 0
    itemType = types[type_index].dataType

    return itemType

### Returns a boolean indicates if existis column into data frame.
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
    
    items = json.loads(data)
    for item in items:
        product_categories = _parse_product_categories(item.get('productCategories', {}))
        item['productCategories'] = product_categories

    return items

##### Register functions as Spark UDFs
udf_getData = UserDefinedFunction(convert_item, get_schema_type("Items"))


#### Casting de ItemMetadata
def convert_item_metadata(data):
    if data:
        itemmetadata = json.loads(data)
        len_items_itemmetadata = itemmetadata and len(itemmetadata["items"])
        for i in range(len_items_itemmetadata):
            if ("assemblyOptions" in itemmetadata["items"][i]):
                del itemmetadata["items"][i]["assemblyOptions"]
        
        return itemmetadata
        
##### Register functions as Spark UDFs
udf_convert_item_metadata = UserDefinedFunction(convert_item_metadata, get_schema_type("ItemMetadata"))


#### Casting RateAndBenefits
def convert_ratesandbenefits(data):
    KEY_IDENTIFIERS = "rateAndBenefitsIdentifiers"
    KEY_MATCH_PARAMS = "matchedParameters"
    KEY_ADDINFO = "additionalInfo"
    data = json.loads(data)
    if data and KEY_IDENTIFIERS in data:
        len_key_identifiers = len(data[KEY_IDENTIFIERS]) if data[KEY_IDENTIFIERS] else 0
        for i in range(len_key_identifiers):
            if KEY_MATCH_PARAMS in data[KEY_IDENTIFIERS][i]:
                del data[KEY_IDENTIFIERS][i][KEY_MATCH_PARAMS]
            if KEY_ADDINFO in data[KEY_IDENTIFIERS][i]:
                del data[KEY_IDENTIFIERS][i][KEY_ADDINFO]
    
    return data
        
##### Register functions as Spark UDFs
udf_convert_ratesandbenefits = UserDefinedFunction(convert_ratesandbenefits, get_schema_type("RatesAndBenefitsData"))


#### Casting CustomData
def convert_custom_data(data):
    KEY_CUSTOMAPP = "customApps"
    KEY_FIELDS = "fields"
    KEY_EXTRA_CONTENT = "cart-extra-context"
    
    customdata = data and json.loads(data)
    if customdata and KEY_CUSTOMAPP in customdata:
        for i in range(len(customdata[KEY_CUSTOMAPP])):
            if KEY_FIELDS in customdata[KEY_CUSTOMAPP][i] and\
                KEY_EXTRA_CONTENT in customdata[KEY_CUSTOMAPP][i][KEY_FIELDS]:
                    del customdata[KEY_CUSTOMAPP][i][KEY_FIELDS][KEY_EXTRA_CONTENT]
    return customdata
        
##### Register functions as Spark UDFs
udf_convert_custom_data = UserDefinedFunction(convert_custom_data, get_schema_type("CustomData"))


#### Structuring data values that is not string
def load_objects_json(df):
    def _load_json(obj):
        try:
            return json.loads(obj)
        except:
            return obj
    
    new_df = df
    for field in structured_df.schema.fields:
        field_type = field.dataType
        field_name = field.name
        if field_type != StringType():
            udf_get_transform_data = UserDefinedFunction(_load_json, field_type)
            new_df = df.withColumn(field_name, udf_get_transform_data(field_name))

    return new_df


#### Remove keys with Attachment content, buecause it is not necessary for development. 
def remove_attachments(df):
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
        
    def _field_cleansing(field):
        if type(field) is dict:
            field = _remove_attachments_key(field)
        return field

    ATTACHMENT = "attachment"
    new_df = df
    for field in new_df.schema.fields:
        field_type = field.dataType
        field_name = field.name
        if ATTACHMENT in field_name.lower():
            new_df = new_df.drop(field_name)
        elif field_type != StringType():
            udf_get_transform_data = UserDefinedFunction(lambda f: _field_cleansing(f), field_type)
            new_df = new_df.withColumn(field_name, udf_get_transform_data(field_name))

    return new_df


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

#### Register functions as Spark UDFs 
udf_getYear = UserDefinedFunction(getYear, StringType())
udf_getMonth = UserDefinedFunction(getMonth, StringType())
udf_getDay = UserDefinedFunction(getDay, StringType())

### Create the Columns for the Partitions
def create_partition_columns(df):
    new_data_frame = df
    new_data_frame = new_data_frame.withColumn('YEAR', udf_getYear(new_data_frame.LastChange, new_data_frame.CreationDate))
    new_data_frame = new_data_frame.withColumn('MONTH', udf_getMonth(new_data_frame.LastChange, new_data_frame.CreationDate))
    new_data_frame = new_data_frame.withColumn('DAY', udf_getDay(new_data_frame.LastChange, new_data_frame.CreationDate))

    return new_data_frame

### Converting data with spark
def struct_data_frame(df):
    new_data_frame = df
    new_data_frame = new_data_frame.withColumn('Items', udf_getData("Items")) if has_column(new_data_frame, 'Items') else new_data_frame
    new_data_frame = new_data_frame.withColumn("ItemMetadata", udf_convert_item_metadata("ItemMetadata")) if has_column(new_data_frame, 'ItemMetadata') else new_data_frame
    new_data_frame = new_data_frame.withColumn("RatesAndBenefitsData", udf_convert_ratesandbenefits("RatesAndBenefitsData")) if has_column(new_data_frame, 'RatesAndBenefitsData') else new_data_frame
    new_data_frame = new_data_frame.withColumn("CustomData", udf_convert_custom_data("CustomData")) if has_column(new_data_frame, 'CustomData') else new_data_frame
    new_data_frame = load_objects_json(new_data_frame)
    new_data_frame = remove_attachments(new_data_frame)

    return new_data_frame


def main():
    hexadecimal_sequence = '0123456789ABCDEF'
    index_folder = '0'
    for hexadecimal in hexadecimal_sequence:
        ### Reading data from Checkout History
        directory_path = index_folder + hexadecimal + '_CheckoutOrder/'
        history_data_path = 's3://vtex-analytics-import/vtex-checkout-versioned/' + directory_path + '*/id/*'
        df = spark.read.json(history_data_path)

        df = struct_data_frame(df)
        df = create_partition_columns(df)

        ### Writing data into S3 bucket
        #### Save table to S3 using Parquet format and partitioning by defined columns
        df.repartition('YEAR','MONTH','DAY','InstanceId')\
            .write\
            .partitionBy('YEAR','MONTH','DAY','InstanceId')\
            .mode('append')\
            .parquet('s3://vtex.datalake/consumable_tables/checkout')

main()
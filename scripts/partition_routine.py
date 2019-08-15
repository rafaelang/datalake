# coding: utf-8

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

import json
import time


### Config SparkContext
spark = SparkSession.builder.master("local").appName("Partition").getOrCreate()

### Getting Schema's Interface from Checkout Structured Json
structured_jsons_path = 's3://vtex.datalake/structured_json/checkout/00_CheckoutOrder/0{*}/id/*'
structured_df = spark.read.json(structured_jsons_path)
schema = structured_df.schema


### Reading dataset to partition (last one hour of data streamed by firehose)
## TODO: fulfillment
def get_dataset_path(timezone_str='UTC'):
    from datetime import datetime, timedelta
    from pytz import timezone

    now = datetime.now(timezone(timezone_str))
    last_hour_date = now - timedelta(hours=1)
    
    path = "s3://vtex.datalake/stage/checkout_data/checkourtorder"\
        + "/ingestion_year=" + str(last_hour_date.year) \
        + "/ingestion_month=" + str(last_hour_date.month) \
        + "/ingestion_day=" + str(last_hour_date.day) \
        + "/ingestion_hour=" + str(last_hour_date.hour)
    return path


history_datapath = 's3://vtex-analytics-import/vtex-checkout-versioned/00_CheckoutOrder/003{*}/id/*'
df = spark.read.json(history_datapath)


### Auxiliary Functions
def has_column(df, col):
    from pyspark.sql.utils import AnalysisException
    try:
        df[col]
        return True
    except AnalysisException:
        return False

    
### Converting data with spark

#### Casting Items

##### Get Item Type
types = filter(lambda f: f.name == "Items", structured_df.schema.fields)
itemType = types[0].dataType

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

udf_getData = UserDefinedFunction(convert_item, itemType)

if has_column(df, 'Items'):
    df = df.withColumn('Items', udf_getData("Items"))


#### Casting de ItemMetadata

types = filter(lambda f: f.name == "ItemMetadata", structured_df.schema.fields)
item_metadata_type = types[0].dataType

def convert_item_metadata(data):
    if data:
        itemmetadata = json.loads(data)
        len_items_itemmetadata = itemmetadata and len(itemmetadata["items"])
        for i in range(len_items_itemmetadata):
            if ("assemblyOptions" in itemmetadata["items"][i]):
                del itemmetadata["items"][i]["assemblyOptions"]
        return itemmetadata
        
udf_convert_item_metadata = UserDefinedFunction(convert_item_metadata, item_metadata_type)

if has_column(df, "ItemMetadata"):
    df = df.withColumn("ItemMetadata", udf_convert_item_metadata("ItemMetadata"))


#### Casting RateAndBenefits

types = filter(lambda f: f.name == "RatesAndBenefitsData", structured_df.schema.fields)
rateandbenefits_type = types[0].dataType

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
        
udf_convert_ratesandbenefits = UserDefinedFunction(convert_ratesandbenefits, rateandbenefits_type)

if has_column(df, 'RatesAndBenefitsData'):
    df = df.withColumn("RatesAndBenefitsData", udf_convert_ratesandbenefits("RatesAndBenefitsData"))


#### Casting CustomData

types = filter(lambda f: f.name == "CustomData", structured_df.schema.fields)
customdata_type = types[0].dataType

def convert_customdata(data):
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
        
udf_convert_customdata = UserDefinedFunction(convert_customdata, customdata_type)

if has_column(df, 'CustomData'):
    df = df.withColumn("CustomData", udf_convert_customdata("CustomData"))


#### Structuring the remaining data that is not string

def load_json(obj):
    try:
        return json.loads(obj)
    except:
        return obj
    
for field in structured_df.schema.fields:
    field_type = field.dataType
    field_name = field.name
    if field_type != StringType():
        udf_get_transform_data = UserDefinedFunction(load_json, field_type)
        df = df.withColumn(field_name, udf_get_transform_data(field_name))

#### Casting Attachment

ATTACHMENT = "attachment"

def remove_attachments(dic):    
    dic_copy = dic.copy()
    for key in dic_copy:
        if(ATTACHMENT in key.lower()):
            del dic[key]
        elif(type(dic_copy[key]) == dict):
            remove_attachments(dic[key])
        elif(type(dic_copy[key]) == list):
            for item in dic_copy[key]:
                if(type(item) == dict):
                    remove_attachments(item)
    return dic
    
def field_cleansing(field, cleansing_func=remove_attachments):
    if type(field) is dict:
        field = cleansing_func(field)
    return field

for field in df.schema.fields:
    field_type = field.dataType
    field_name = field.name
    if ATTACHMENT in field_name.lower():
        df = df.drop(field_name)
    elif field_type != StringType():
        udf_get_transform_data = UserDefinedFunction(lambda f: field_cleansing(f), field_type)
        df = df.withColumn(field_name, udf_get_transform_data(field_name))


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

# Register functions as Spark UDFs 
udf_getYear = UserDefinedFunction(getYear, StringType())
udf_getMonth = UserDefinedFunction(getMonth, StringType())
udf_getDay = UserDefinedFunction(getDay, StringType())

# Create the Columns for the Partitions
df = df.withColumn('YEAR', udf_getYear(df.LastChange, df.CreationDate))
df = df.withColumn('MONTH', udf_getMonth(df.LastChange, df.CreationDate))
df = df.withColumn('DAY', udf_getDay(df.LastChange, df.CreationDate))


### Writing data into S3 bucket

# Save table to S3 using Parquet format and partitioning by defined columns
df.coalesce(1).write.partitionBy(['YEAR','MONTH','DAY','InstanceId']).mode('append').parquet('s3://vtex.datalake/consumable_tables/checkout/')








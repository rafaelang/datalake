import logging
import os
import json
import boto3
import urllib
import re
from gen_struct import transform_struct_json


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


s3 = boto3.client('s3')
firehose = boto3.client('firehose')


ENABLE_WRITE_STRUCTURED_JSON = int(os.getenv('ENABLE_WRITE_STRUCTURED_JSON'))
TABLENAME_RAW_FULFILLMENT = 'FulfillmentOrder'
TABLENAME_RAW_CHECKOUT = 'CheckoutOrder'


def contains_id_path(path):
    '''
        Returns a boolean in which True indicates path contains "/id/", False otherwise.
    '''
    id_path = "/id/"
    is_match = bool(re.search(id_path, path))
    
    return is_match


def save_structured_json_obj_s3(structured_json_prefix, object_path, object_content_body):
    '''
        Saves a structured json object in vtex.datalake Bucket.
    '''
    file_path_dest = structured_json_prefix + object_path
    s3client = boto3.resource('s3')
    object = s3client.Object('vtex.datalake', file_path_dest)
    object.put(Body=json.dumps(object_content_body))


def get_structured_json_prefix(source_key):
    '''
        Returns path's prefix for file destination.
    '''
    structured_json_prefix = "structured_json/"
    if TABLENAME_RAW_FULFILLMENT in source_key:
        structured_json_prefix += "fulfillment/"
    elif TABLENAME_RAW_CHECKOUT in source_key:
        structured_json_prefix += "checkout/"
    return structured_json_prefix


def get_s3_obj_content(source_bucket, source_key):
    object_content = s3.get_object(Bucket=source_bucket, Key=source_key)            
    object_content_body = json.loads(object_content['Body'].read())
    return object_content_body  


def handler(event, context):
    '''
        Function that is called when lambda is triggered.
    '''
    logger.info('Running Lambda Function {}::{}'.format(context.function_name, context.function_version))
    logger.debug('Processing event:\n{}'.format(json.dumps(event, indent=4, sort_keys=True)))

    try:
        sns_notification = json.loads(event['Records'][0]['Sns']['Message'])
        s3_notification = sns_notification['Records'][0]['s3']        
        source_bucket = s3_notification['bucket']['name']        
        file_path_source = s3_notification['object']['key']

        # Only files under a '/id' directory are desired.
        if (contains_id_path(file_path_source)):
            
            source_key = urllib.parse.unquote_plus(file_path_source)
            object_content_body = get_s3_obj_content(source_bucket, source_key)
            structured_json = transform_struct_json(object_content_body)            
            structured_json_prefix = get_structured_json_prefix(source_key)
            
            if ENABLE_WRITE_STRUCTURED_JSON:
                save_structured_json_obj_s3(structured_json_prefix, file_path_source, structured_json)


    except Exception as e:
        logger.error('Error readind object from S3 ' + str(e))
        raise e
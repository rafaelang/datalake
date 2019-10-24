import logging
import os
import json
import boto3
import urllib
from cleanser import clean_struct_json


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


s3 = boto3.client('s3')
firehose = boto3.client('firehose')


ENABLE_WRITE_CLEANSED_JSON = int(os.getenv('ENABLE_WRITE_CLEANSED_JSON'))


def save_on_s3(object_content_body, file_path_dest, bucket="vtex.datalake"):
    '''
        Saves a cleansed json object in vtex.datalake Bucket.
    '''
    try:    
        s3client = boto3.resource('s3')
        object = s3client.Object(bucket, file_path_dest)
        object.put(Body=json.dumps(object_content_body))
        return True
    except:
        return False


def get_s3_obj_content(source_bucket, source_key):
    '''
        Loads the checkout/fulfillment order from S3 as a json object and returns it.
    '''    
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

        source_key = urllib.parse.unquote_plus(file_path_source)
        object_content_body = get_s3_obj_content(source_bucket, source_key)
        cleansed_json = clean_struct_json(object_content_body)
        file_path_dest = "raw_data/orders/cleansed_stage/" + source_key
                        
        if ENABLE_WRITE_CLEANSED_JSON:
            save_on_s3(cleansed_json, file_path_dest)

    except Exception as e:
        logger.error('Error readind object from S3 ' + str(e))
        raise e
import json
import logging
import os

# logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))

ATTACHMENT = "attachment"
JSON_KEY_ITEMMETADATA = "ItemMetadata"
JSON_KEY_RATESANDBDATA = "RatesAndBenefitsData"
JSON_KEY_CUSTOMDATA = "CustomData"


def remove_attachments(dic):
    '''
        Recursively removes fields whose name contains `attachment` as a substring.

        Args:
            dic (dict): json as object.
        Returns:
            a copy of the input dict without any `attachment` field.
    '''    
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


def clean_itemmetadata(itemmetadata):
    '''
        Remove malformed inner fields from json object (assemblyOptions).

        Args:
            itemmetadata (dict): order item's metadata.
        Returns:
            the input argument but with structure changes inner fields.         
    '''    
    len_items_itemmetadata = len(itemmetadata["items"])
    for i in range(len_items_itemmetadata):
        if ("assemblyOptions" in itemmetadata["items"][i]):
            del itemmetadata["items"][i]["assemblyOptions"]
    return itemmetadata
    
    
def clean_ratesandbenefitsdata(rtbndata):
    '''
        Remove malformed inner fields from json object (matchedParameters e additionalInfo).

        Args:
            rtbndata (list[dict]): order item's metadata list.
        Returns:
            the input array but with structure changes in each element.         
    '''     
    KEY_IDENTIFIERS = "rateAndBenefitsIdentifiers"
    KEY_MATCH_PARAMS = "matchedParameters"
    KEY_ADDINFO = "additionalInfo"
    for i in range(len(rtbndata[KEY_IDENTIFIERS])):
        if KEY_MATCH_PARAMS in rtbndata[KEY_IDENTIFIERS][i]:
            del rtbndata[KEY_IDENTIFIERS][i][KEY_MATCH_PARAMS]
        if KEY_ADDINFO in rtbndata[KEY_IDENTIFIERS][i]:            
            del rtbndata[KEY_IDENTIFIERS][i][KEY_ADDINFO]
    return rtbndata


def clean_customdata(customdata):
    '''
        Remove malformed inner fields from json object (customApps.cart-extra-contex).

        Args:
            customdata (dict): order item's metadata.
        Returns:
            the input argument but with structure changes in inner fields.
    '''  
    KEY_CUSTOMAPP = "customApps"
    KEY_FIELDS = "fields"
    KEY_EXTRA_CONTENT = "cart-extra-context"
    if KEY_CUSTOMAPP in customdata:
        for i in range(len(customdata[KEY_CUSTOMAPP])):
            logger.info('menorme {}'.format(customdata[KEY_CUSTOMAPP]))
            if KEY_FIELDS in customdata[KEY_CUSTOMAPP][i] and\
                KEY_EXTRA_CONTENT in customdata[KEY_CUSTOMAPP][i][KEY_FIELDS]:
                    del customdata[KEY_CUSTOMAPP][i][KEY_FIELDS][KEY_EXTRA_CONTENT]
    return customdata
    

def clean_struct_json(raw_json):
    '''
        Remove some fields (know for problems like special chars os blank space on fields keys), 
        turning data query-able.
    '''
    cleansed_json = dict()
    for key in raw_json.keys():
        raw_value = raw_json.get(key)
        try:
            cleansed_json[key] = json.loads(raw_value)
                
            if key == JSON_KEY_ITEMMETADATA:
                cleansed_json[key] = clean_itemmetadata(cleansed_json[key])
                
            if key == JSON_KEY_RATESANDBDATA:
                cleansed_json[key] = clean_ratesandbenefitsdata(cleansed_json[key])
                
            if key == JSON_KEY_CUSTOMDATA:
                cleansed_json[key] = clean_customdata(cleansed_json[key])
                
        except json.decoder.JSONDecodeError:  # when raw_value is a simple string
            cleansed_json[key] = raw_value
        except TypeError:
            cleansed_json[key] = raw_value
    
    cleansed_json = remove_attachments(cleansed_json)
    logger.info('cleansed_json {}'.format(cleansed_json))
    
    return cleansed_json
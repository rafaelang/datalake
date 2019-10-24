import json
import logging
import os

# logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))

ATTACHMENT = "attachment"
JSON_KEY_ITEMMETADATA = "ItemMetadata"
JSON_KEY_ITEMS = "Items"
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


def transform_items(items):
    '''
        Transform structure of inner fields with the aim of turn them query-able.
        
        Args:
            items (list[dict]): order items list.
        Returns:
            the input array but with structure changes in each element.        
    '''        
    def transform_product_categories(raw_product_categories):
        '''
            Transform the dictionary structure of the 'productCategories' field 
            to a list where each element corresponds to a key and value 
            from the previous structure ({id: key, name: value})
        '''           
        product_categories = []
        for k,v in raw_product_categories.items():
            product_categories.append({
                'id': k,
                'name': v
            })
        return product_categories
    
    for item in items:
        product_categories = transform_product_categories(item.get('productCategories', {}))
        item['productCategories'] = product_categories
        
    return items


def transform_itemmetadata(itemmetadata):
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
    
    
def transform_ratesandbenefitsdata(rtbndata):
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


def transform_customdata(customdata):
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
            if KEY_FIELDS in customdata[KEY_CUSTOMAPP][i] and\
                KEY_EXTRA_CONTENT in customdata[KEY_CUSTOMAPP][i][KEY_FIELDS]:
                    del customdata[KEY_CUSTOMAPP][i][KEY_FIELDS][KEY_EXTRA_CONTENT]
    return customdata
    

def transform_struct_json(raw_json):
    '''
        Cast some fields and change structure of others. 
        All fields in raw_json are saved as string, even thouth they are boolean, array or nested 
        json. So this functions cast fields to appropriate type. Also change structures to turn 
        them query-able, deleting keys with special chars.
    '''
    structured_json = dict()
    for key in raw_json.keys():
        raw_value = raw_json.get(key)
        try:
            structured_json[key] = json.loads(raw_value)
            
            if key == JSON_KEY_ITEMS:
                structured_json[key] = transform_items(structured_json[key])
                
            if key == JSON_KEY_ITEMMETADATA:
                structured_json[key] = transform_itemmetadata(structured_json[key])
                
            if key == JSON_KEY_RATESANDBDATA:
                structured_json[key] = transform_ratesandbenefitsdata(structured_json[key])
                
            if key == JSON_KEY_CUSTOMDATA:
                structured_json[key] = transform_customdata(structured_json[key])
                
        except json.decoder.JSONDecodeError:  # when raw_value is a simple string
            structured_json[key] = raw_value
        except TypeError:
            structured_json[key] = raw_value
    
    structured_json = remove_attachments(structured_json)
    logger.info('structured_json {}'.format(structured_json))
    
    return structured_json
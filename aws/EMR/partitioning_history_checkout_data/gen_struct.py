import json
import os

ATTACHMENT = "attachment"
JSON_KEY_ITEMMETADATA = "ItemMetadata"
JSON_KEY_ITEMS = "Items"
JSON_KEY_RATESANDBDATA = "RatesAndBenefitsData"
JSON_KEY_CUSTOMDATA = "CustomData"


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


def parse_items(items):
    def _parse_product_categories(raw_product_categories):
        product_categories = []
        for k,v in raw_product_categories.items():
            product_categories.append({
                'id': k,
                'name': v
            })
        return product_categories
    
    for item in items:
        product_categories = _parse_product_categories(item.get('productCategories', []))
        item['productCategories'] = product_categories
        
    return items


def transform_itemmetadata(itemmetadata):
    len_items_itemmetadata = len(itemmetadata["items"])
    for i in range(len_items_itemmetadata):
        if ("assemblyOptions" in itemmetadata["items"][i]):
            del itemmetadata["items"][i]["assemblyOptions"]
    return itemmetadata
    
    
def transform_ratesandbenefitsdata(data):
    KEY_IDENTIFIERS = "rateAndBenefitsIdentifiers"
    KEY_MATCH_PARAMS = "matchedParameters"
    KEY_ADDINFO = "additionalInfo"
    for i in range(len(data[KEY_IDENTIFIERS])):
        if KEY_MATCH_PARAMS in data[KEY_IDENTIFIERS][i]:
            del data[KEY_IDENTIFIERS][i][KEY_MATCH_PARAMS]
            del data[KEY_IDENTIFIERS][i][KEY_ADDINFO]
    return data


def transform_customdata(customdata):
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
        Transforms to json struct. 
    '''
    structured_json = dict()
    for key in raw_json.keys():
        raw_value = raw_json.get(key)
        try:            
            structured_json[key.encode("utf-8")] = json.loads(raw_value)
            
            if key == JSON_KEY_ITEMS:
                structured_json[key] = parse_items(structured_json[key])
                
            if key == JSON_KEY_ITEMMETADATA:
                structured_json[key] = transform_itemmetadata(structured_json[key])
                
            if key == JSON_KEY_RATESANDBDATA:
                structured_json[key] = transform_ratesandbenefitsdata(structured_json[key])
                
            if key == JSON_KEY_CUSTOMDATA:
                structured_json[key] = transform_customdata(structured_json[key])
                
        except TypeError:
            structured_json[key] = raw_value
        except ValueError:
            structured_json[key] = raw_value
    
    structured_json = remove_attachments(structured_json)
    
    return structured_json
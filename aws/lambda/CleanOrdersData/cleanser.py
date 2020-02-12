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
JSON_KEY_PAYMENT = "PaymentData"


def remove_attachments(dic):
    '''
        Recursively removes fields whose name contains `attachment` as a substring.

        Args:
            dic (dict): json as object.
        Returns:
            a copy of the input dict without any `attachment` field.
    '''    
    dic_copy = dic.copy()
    for key in dic:
        if(ATTACHMENT in key.lower()):
            del dic_copy[key]
        elif(type(dic[key]) == dict):
            remove_attachments(dic_copy[key])
        elif(type(dic[key]) == list):
            for item in dic_copy[key]:
                if(type(item) == dict):
                    remove_attachments(item)
    return dic_copy


def clean_itemmetadata(itemmetadata):
    '''
        Remove malformed inner fields from json object (assemblyOptions).

        Args:
            itemmetadata (dict): order item's metadata.
        Returns:
            the input argument but with structure changes inner fields.         
    '''    
    itemmetadata_cp = itemmetadata
    len_items_itemmetadata = len(itemmetadata_cp["Items"])
    for i in range(len_items_itemmetadata):
        if ("AssemblyOptions" in itemmetadata_cp["Items"][i]):
            del itemmetadata_cp["Items"][i]["AssemblyOptions"]
    return itemmetadata_cp
    
    
def clean_ratesandbenefitsdata(rtbndata):
    '''
        Remove malformed inner fields from json object (matchedParameters e additionalInfo).

        Args:
            rtbndata (list[dict]): order item's metadata list.
        Returns:
            the input array but with structure changes in each element.         
    '''     
    KEY_IDENTIFIERS = "RateAndBenefitsIdentifiers"
    KEY_MATCH_PARAMS = "MatchedParameters"
    KEY_ADDINFO = "AdditionalInfo"

    rtbndata_cp = rtbndata
    for i in range(len(rtbndata_cp[KEY_IDENTIFIERS])):
        if KEY_MATCH_PARAMS in rtbndata_cp[KEY_IDENTIFIERS][i]:
            del rtbndata_cp[KEY_IDENTIFIERS][i][KEY_MATCH_PARAMS]
        if KEY_ADDINFO in rtbndata_cp[KEY_IDENTIFIERS][i]:            
            del rtbndata_cp[KEY_IDENTIFIERS][i][KEY_ADDINFO]
    return rtbndata_cp


def clean_paymentdata(paydata):
    KEY_TRANSACTIONS = "Transactions"
    KEY_CONN_RESP = "connectorResponses"
    KEY_PAYMENTS = "Payments"

    paydata_cp = paydata
    if KEY_TRANSACTIONS in paydata_cp:
        for i in range(len(paydata_cp[KEY_TRANSACTIONS])):
            if KEY_PAYMENTS in paydata_cp[KEY_TRANSACTIONS][i]:
                payments = paydata_cp[KEY_TRANSACTIONS][i][KEY_PAYMENTS]
                for j in range(len(payments)):
                    if KEY_CONN_RESP in payments[j]:
                        del paydata_cp[KEY_TRANSACTIONS][i][KEY_PAYMENTS][j][KEY_CONN_RESP]
    return paydata_cp

    
def clean_struct_json(raw_json):
    '''
        Remove some fields (know for problems like special chars os blank space on fields keys), 
        turning data query-able.
    '''
    cleansed_json = dict()
    for key in raw_json.keys():
        raw_value = raw_json.get(key)
        
        try:
            cleansed_json[key] = raw_value
                
            if key == JSON_KEY_ITEMMETADATA:
                cleansed_json[key] = clean_itemmetadata(cleansed_json[key])
                
            if key == JSON_KEY_RATESANDBDATA:
                cleansed_json[key] = clean_ratesandbenefitsdata(cleansed_json[key])
                
            if key == JSON_KEY_CUSTOMDATA:
                del cleansed_json[JSON_KEY_CUSTOMDATA]
                
            if key == JSON_KEY_PAYMENT:
                cleansed_json[key] = clean_paymentdata(cleansed_json[key])
            
                
        except json.decoder.JSONDecodeError:  # when raw_value is a simple string
            cleansed_json[key] = raw_value
        except TypeError:
            cleansed_json[key] = raw_value
    
    cleansed_json = remove_attachments(cleansed_json)
    logger.info('cleansed_json {}'.format(cleansed_json))
    
    return cleansed_json
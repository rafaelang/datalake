### Description

This function is triggered always that new data is replicated from the vtex-orders-index bucket (at VTEX general account) to vtex-orders-index-rawdata (VTEX Analytics account).
In this Orders Data some fields has bad keys (special chars, for example). So, this lambda functions copies every new json object that comes to vtex-orders-index-rawdata, remove theses fields with bad keys and save them on s3://vtex.datalake/raw_data/orders/cleansed_stage/

### Execution Role

There is an role called lambda-s3-role with the following polices:   

- AWSLambdaExecute (_allows_ to _write_ on _log_ and to _get or put_ objects on _s3_).


### Env vars

This lambda needs these vars: 

    - ENABLE_WRITE_STRUCTURED_JSON
        - Enables to write structured files under 'vtex.datalake/structure_json path
        - Should be 0 (zero) or 1 (one)
            - When 0, files are NOT written
            - When 1, file are written.


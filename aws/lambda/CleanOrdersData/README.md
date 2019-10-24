### Description

This function is triggered always that new data is replicated from the vtex-checkout-versioned (VTEX account) to vtex-checkout-versioned-rawdata (VTEX Analytics account).
This Checkout Data (checkoutOrders and fulfillmentOrders) is unstructured — json files whose values are always string, even thought it should be nested jsons. So, this lambda functions transform these json, in the sense of load what is a string value to what it should be, whether a string, an array, a boolean, a nested json...

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


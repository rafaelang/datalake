In the data exploration process, some abnormalities were found in the structure of the observations. Here we record these errors we found and what we did to deal with them.
In general, theses abnormalities are related to malformed keys on json files, like white space and special chars (@). They are problematic and must be deleted or updated because some frameworks/services like Hive and Athena do not retrieve data from files with this malformed keys. Errors are reported instead.

**Orders Data**

Orders data is built on top of other data sources, including Checkout data. Given this, some fields that are unstructured in Checkout are also in Orders.

  - `productCategories` field within` Items`: keys are identifiers and values are category names. *(Fixed: creation of an object with keys: id and name)*;

  - Any field that contains the string `attachment`: in all cases, the keys are malformed as they have either white space or special characters (eg: at sign). *(Fixed: All keys that have the attachment have been deleted)*;

  - `AssemblyOptions` field inside `ItemMetadata`: just like fields with `attachment`, there is an internal field called` inputValue` which also contains malformed keys, that is, with special characters; *(Fixed: assemblyOptions has been deleted from remarks).*

  - `RateAndBenefitsIdentifiers` field within `ratesandbenefitsdata` contains the `matchedParameters` field which, like the previous ones, has malformed keys with special characters. *(Fixed: matchedParameters has been deleted from remarks).*

  - `CustomData` field had several non-standard fields (similar to attachements). Since these were unimportant fields, we deleted this column from the final structure of *consumable_tables*

  - `CommercialConditionData` field likewise has different structure types. _It was also deleted_ from the final structure.

  - Array `Transactions` within` paymentdata` contains sensitive user data (Payments) and special character data (Payments / connectorResponses). _This *transactions* field was deletede_.


All these bugs have been fixed in both the lambda that structures files and the script that partitions/structures migration data
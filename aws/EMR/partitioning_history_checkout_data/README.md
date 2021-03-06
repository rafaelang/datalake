### Description

The function `struct_and_partition_history_checkout` must be used when you need to struct data from history vtex, in which the data has saved in raw. Therefore, this function will convert the raw to json objects, structing and partition the by `Year`, `Month` and `Day`.  
This function should be runned in aws EMR, because it used **Spark** application to executed it, so EMR configurates the enviroment necessary for the Spark, besides you can configurate the cluster to the size necessary according the data size.  
  
On the order hand, the function `gen_config_cluster_spark` is used to get the Spark's configurations according Cluster Core's configurations, which will be used to create the cluster in EMR.


### Execution

To execute the function `struct_and_partition_history_checkout`, must create a cluster in aws EMR with Spark installed and configurates the Spark with json gotten in `gen_config_cluster_spark`.


### Args

This `struct_and_partition_history_checkout` needs this arg to run:

- --directory_path 
    - The `--directory_path` is the hexadecimal path to be read, ex: 00_CheckoutOrder.

- --destination_path
    - The `--destination_path` is the path where data to be written, ex: consumable_tables/fulfillment.
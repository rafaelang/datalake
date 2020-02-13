# Checkout Historical Data Migration

- [Preamble](#Preamble)
- [Data Migration](#Data-Migration)
- [Data Transformation](#Data-Transformation)
    - [Faced problems](#Faced-problems)

## Preamble

After implementing _pipeline_ for structuring and partitioning Checkout data, every new order you create is now saved in a first version ready for consumption / query in your Analytics account. However, we still had the need to retrieve all orders datas dated before implementing our _pipeline_. In this regard, this document describes how the Checkout historical data migration process took place and how we transformed this data to have the same format as that produced by _pipeline_ for datalake.

The informations here described are similar to the ones on [OMS data migration](../oms/README.md). We kept here just highlighted differences.

## Data Migration

Checkout data is originally saved on an s3 bucket in Virginia region. The bucket in the Analytics account where we save transformed data is saved in Ohio region. So the problem we are trying to solve in this part is **copying data between buckets in different regions**.

For migration, we used EC2 service to provision **16 instances** of high capacity  (**type: m5.4xlarge, with 16 VCPus, 64 Mem**). Why 16 instances? We would copy 512 folders in total from the source bucket (216 checkoutOrder folders and 216 fulfillmentOrder, according to the bucket folder structure). We decided that each machine would be responsible for copying 32 folders, so 16 * 32 = 512. We reached to  number 32 after a few experiments, and we realized that one machine could copy 32 folders within one day, which we could tolerate.

This document is based on [docs](https://docs.google.com/document/d/1LFyubm8vLXcrdPxL09WxzKsvQ-HMmirGIqZBZazuCf0/edit#) created when specific checkout data is migrated.

## Data Transformation

Once copied to the Analytics account, Checkout data is raw. They need to be transformed (structured) and partitioned. Since this is a very large dataset (approx 4 terabytes), we decided to use a [script](https://github.com/vtex/datalake/tree/master/aws/EMR/partitioning_history_checkout_data) in pyspark.
To run spark jobs, we decided to do it in the context of a cluster created with aws EMR service. But with which configuration to create this cluster, so as to best utilize machine resources and finish processing successfully?
Throughout the experiments we have had some memory overflow issues or resource misuse or excessive run time issues required to complete jobs. After some testing, we come to the following configuration:

We required 32 clusters, each with the following configuration:
   - 1 machine r5.2xlarge type Master 
   - 2 machines c5.4xlarge type Core

> **WARNING**: Importantly, when using machines with large resources, spark does not always run jobs using this avaible resources. You must explicitly state the amount of memory spark can consume, the amount of parallelization (CPU's), etc. Therefore, we use [this script](https://github.com/vtex/datalake/blob/master/aws/EMR/partitioning_history_checkout_data/gen_config_cluster_spark.py) to configure clusters to best use instance resources.

Each cluster is responsible for transforming 16 folders ("divide and conquer"). For example:
- cluster A gets the folders 00_CheckourtOrder, 01_CheckoutOrder, ..., 0E_CheckoutOrder, 0F_CheckoutOrder
- cluster B is responsible for 40_FulfillmentOrder, 41_FulfillmentOrder, ..., 4E_FulfillmentOrder, 4F_FulfillmentOrder.

Each folder must correspond to a spark job. Therefore, once a cluster is created, each folder must have an associated _step_. This _step_ means exactly submitting the partitioning script to a folder (eg 41_FulfillmentOrder). This is possible because [script](https://github.com/vtex/datalake/tree/master/aws/EMR/partitioning_history_checkout_data) takes a folder as an argument ([read](https://github.com/vtex/datalake/tree/master/aws/EMR/partitioning_history_checkout_data) more about how the script works). We use _[another script](https://github.com/vtex/datalake/tree/master/scripts/checkout_partition_history)_ to create all the clusters needed to transform and partition Checkout data, indicating in each one already a step specific to a subfolder, passed as an argument.

### Faced problems 

Some problems arose during the cluster transformation and configuration processes. Here we will report what these problems were and how we solved them.

The process of structuring the checkoutOrder data went smoothly with the data itself. The same did not happen with the fulfillmentOrder data.
   - Some steps were not performed because folders (each step is related to a folder) did not exist in our buckets on s3. As a result, we found that the data migration process (described above) failed for three fulfillmentOrder folders (E9__FulfillmentOrder, ED__FulfillmentOrder, FA__FulfillmentOrder).
   - Some folders (50_FulfillmentOrder, F0_FulfillmentOrder, D0_FulfillmentOrder, C0_FulfillmentOrder) failed after two minutes of spark job execution. This is thought to be an AWS infrastructure-related load issue as we fired all 16 clusters at the same time, and the first few steps (* 0_FulfillmentOrder) of some clusters failed without a specific error message. Also, at the end of the process, we just ran these 4 steps again (without any changes) and they ran without any errors.
   - There were failures in folders [22, 0D, 1D, 83, FE, F9] _FulfillmentOrder, related to a problem that occurred on October 31, 2018 at 14h that generated files with names with special characters. Spark crashed when attempting to read these files (it was reported that the paths to the files did not exist). Fortunately, the error messages indicated which _path_ had broken execution. We decided to rename the specific files, removing the special characters, and rerun the jobs.
   - The 86_FulfillmentOrder folder structure failed because there were subfolders within the paths below */id/. These subfolders are anomalies within the bucket folder structure. The script tries to read any json just below /id . And then when spark lists the file paths before  read them, the pathes for files within those subfolders are malformed (it expects a json and finds a folder), generating blanks that, in a later attempt to read, lead to nonexistent paths.

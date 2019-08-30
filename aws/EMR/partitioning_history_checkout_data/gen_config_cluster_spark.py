import json
from math import floor, ceil

# Gets all Spark configurations and saves in json file.
def get_spark_configurations(num_vcpus_per_instance, memory_ram_size_per_instance):
    default_config_spark_and_yarn = [
        {
            "Classification": "yarn-site",
            "Properties": {
            "yarn.nodemanager.vmem-check-enabled": "false",
            "yarn.nodemanager.pmem-check-enabled": "false" 
            }
        }       
    ]
    default_config_spark = {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.dynamicAllocation.enabled": "false",
            "spark.driver.memory": "37000M",
            "spark.executor.memory": "37000M",
            "spark.executor.cores": "5",
            "spark.driver.cores": "5",          
            "spark.executor.instances": "8",
            "spark.yarn.executor.memoryOverhead": "5000M",
            "spark.yarn.driver.memoryOverhead": "5000M",
            "spark.memory.fraction": "0.80",
            "spark.memory.storageFraction": "0.30",
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
            "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
            "spark.yarn.scheduler.reporterThread.maxFailures": "5",
            "spark.storage.level": "MEMORY_AND_DISK_SER",
            "spark.rdd.compress": "true",
            "spark.shuffle.compress": "true",
            "spark.shuffle.spill.compress": "true",
            "spark.default.parallelism": "80"
        }
    }

    # Calculating Spark configurations
    executors_cores = 5 # Its recommended by community.

    num_executors_per_instance = floor(float(num_vcpus_per_instance - 1) / executors_cores)
    total_executor_memory = floor(float(memory_ram_size_per_instance) / num_executors_per_instance)
    
    executor_memory = str(int(floor(total_executor_memory * 0.9))) + "g" # 90 percent from this total executor memory to the executor memory
    yarn_memory_overhead = str(int(ceil(total_executor_memory * 0.1)) * 1024) # 10 percent from this total executor memory to the memory overhead, in MegaBytes

    driver_memory = executor_memory
    driver_cores = executors_cores

    executor_instances = int(num_executors_per_instance * num_vcpus_per_instance - 1) # Minus 1, because 1 for driver

    parallelism = executor_instances * executors_cores * 2

    # Insert new configurations into spark properties
    default_config_spark["Properties"]["spark.executor.cores"] = str(executors_cores)
    default_config_spark["Properties"]["spark.executor.memory"] = str(executor_memory)
    default_config_spark["Properties"]["spark.driver.memory"] = str(driver_memory)
    default_config_spark["Properties"]["spark.driver.cores"] = str(driver_cores)
    default_config_spark["Properties"]["spark.executor.instances"] = str(executor_instances)
    default_config_spark["Properties"]["spark.default.parallelism"] = str(parallelism)
    default_config_spark["Properties"]["spark.yarn.driver.memoryOverhead"] = str(yarn_memory_overhead)
    default_config_spark["Properties"]["spark.yarn.executor.memoryOverhead"] = str(yarn_memory_overhead)

    # Saves new configurations in default_config_spark_and_yarn
    default_config_spark_and_yarn.append(default_config_spark)

    # Saving data in new Json File
    with open('config.json', 'w') as f:
        json.dump(default_config_spark_and_yarn, f)

# Gets all Spark configurations from number of vcpus and RAM size.
get_spark_configurations(16, 24) # You need change this parameters to cores' configurations.
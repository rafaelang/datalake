#!/usr/bin/env bash

set -euxo pipefail

cluster_id='j-N12K2PW9VHGN'

app_name="BlackFridayDashboard"

main_class="Main"
jar_name="BlackFridayDashboard-assembly-0.1.jar"
jar_path="target/scala-2.11/${jar_name}"
s3_jar_dir="s3://vtex.datalake/jars/"
s3_jar_path="${s3_jar_dir}${jar_name}"

###################################################

sbt compile

sbt assembly

aws s3 cp ${jar_path} ${s3_jar_dir}

aws emr add-steps --region us-east-2 --cluster-id ${cluster_id} --steps Type=spark,Name=${app_name},Args=[--deploy-mode,cluster,--master,yarn,--class,${main_class},${s3_jar_path},--conf,spark.dynamicAllocation.enabled=true],ActionOnFailure=CONTINUE
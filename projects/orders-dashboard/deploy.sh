#!/usr/bin/env bash

set -euxo pipefail

cluster_id='cluster_id_here'

app_name="BlackFridayDashboard"

main_class="Main"
jar_name="blackfridaydashboard_2.11-0.1.jar"
jar_path="target/scala-2.11/${jar_name}"
s3_jar_dir="s3://vtex-ml/jars/"
s3_jar_path="${s3_jar_dir}${jar_name}"

###################################################

sbt compile

sbt package

aws s3 cp ${jar_path} ${s3_jar_dir}

aws emr add-steps --region us-east-1 --cluster-id ${cluster_id} --steps Type=spark,Name=${app_name},Args=[--deploy-mode,cluster,--master,yarn,--class,${main_class},${s3_jar_path},--conf,spark.dynamicAllocation.enabled=true],ActionOnFailure=CONTINUE
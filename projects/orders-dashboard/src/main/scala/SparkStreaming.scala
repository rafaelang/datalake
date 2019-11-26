import org.apache.spark.streaming.dstream.InputDStream

class Main2 {
  #!/usr/bin/env bash

  set -euxo pipefail

  cluster_id='j-B20DICHVU6RV'

  app_name="WordCount"

  main_class="WordCount"
  jar_name="wordcount_2.12-1.0.jar"
  jar_path="target/scala-2.12/${jar_name}"
  s3_jar_dir="s3://test/jars/"
  s3_jar_path="${s3_jar_dir}${jar_name}"
  ###################################################

  # sbt compile
  #
  # sbt package
  #
  # aws s3 cp ${jar_path} ${s3_jar_dir}

  aws emr add-steps --region us-east-1 --cluster-id ${cluster_id} --steps Type=spark,Name=${app_name},Args=[--deploy-mode,cluster,--master,yarn-cluster,--class,${main_class},${s3_jar_path}],ActionOnFailure=CONTINUE


  "" % "" % ""
  "" % "" % ""
  spark-shell --packages "org.apache.parquet":"parquet-avro":"1.8.1", "org.apache.httpcomponents":"httpclient":"4.5.10"


  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.streaming.{StreamingContext, Seconds, Time}
  import org.apache.spark.sql.SQLContext
  import org.apache.hadoop.fs.Path
  import org.apache.parquet.hadoop.ParquetInputFormat
  import org.apache.parquet.avro.AvroReadSupport
  import org.apache.avro.generic.GenericRecord

  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }

  val ssc  = new StreamingContext(sc, Seconds(15))

  ssc.sparkContext.hadoopConfiguration.set("parquet.read.support.class", "org.apache.parquet.avro.AvroReadSupport")

  val inputDir = "s3://vtex.datalake/raw_data/orders/parquet_stage/ingestion_year=2019/ingestion_month=11/ingestion_day=23/ingestion_hour=15/"

  val stream: InputDStream[(Void, GenericRecord)] = ssc.fileStream[Void, GenericRecord, ParquetInputFormat[GenericRecord]](inputDir, { path: Path => path.toString.endsWith("parquet") }, true, ssc.sparkContext.hadoopConfiguration)

  stream.foreachRDD { (rdd, time) => println(rdd.count())

  }

  val countData = stream.count

  countData.print

  stream.foreachRDD ({ rdd =>rdd.foreach ({ record =>println(record) })})

  stream.transform(rdd => rdd.map(record => record.toString)).foreachRDD((rdd: RDD[String], time: Time) => {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._

    val dataDF = sqlContext.read.parquet(rdd)

    dataDF.printSchema
    dataDF.show
  })

  ssc.start()
  ssc.awaitTermination()





  vtex.datalake/raw_data/orders/parquet_stage/ingestion_year=2019/ingestion_month=11/ingestion_day=23/ingestion_hour=03/

}

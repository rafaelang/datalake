import Helpers.DataframeTransformations
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}

object Main {

  private val date_formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")

  def main(args: Array[String]): Unit = {

    implicit val sparkSession: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("BlackFridayDashboard")
      .getOrCreate()

    implicit val sc: SparkContext = sparkSession.sparkContext
    implicit val ssc: StreamingContext = new StreamingContext(sc, Seconds(60))

    ssc.sparkContext.setLogLevel("ERROR")

    val inputDir: String =
      "s3://vtex.datalake/raw_data/orders/parquet_stage/ingestion_year=2019/ingestion_month=11/ingestion_day=28/ingestion_hour=*/"

    val outputDir: String =
      "s3://vtex.datalake/test/BlackFridayDashboard/outputDir/"

    runSparkContext(inputDir: String, outputDir: String)
  }

  def runSparkContext(inputDir: String, outputDir: String)(
      implicit sparkSession: SparkSession,
      sc: SparkContext,
      ssc: StreamingContext): Unit = {

    ssc.sparkContext.hadoopConfiguration.set(
      "parquet.read.support.class",
      "org.apache.parquet.avro.AvroReadSupport")

    val stream: InputDStream[(Void, GenericRecord)] =
      ssc.fileStream[Void, GenericRecord, ParquetInputFormat[GenericRecord]](
        inputDir, { path: Path =>
          path.toString.endsWith("parquet")
        },
        true,
        ssc.sparkContext.hadoopConfiguration)

    stream.foreachRDD { (rdd, time) =>
      println(rdd.count())
    }

    val countData = stream.count
    countData.print

    stream
      .map(_._2)
      .transform(rdd => rdd.map(record => record.toString))
      .foreachRDD((rdd: RDD[String], time: Time) => {

        import sparkSession.implicits.StringToColumn

        val dataDF: DataFrame = sparkSession.sqlContext.read.json(rdd)

        if (!dataDF.isEmpty) {
          val explodedDF1: DataFrame = dataDF.withColumn("totals", explode($"totals")) //.withColumn("creationdate", $"creationdate".cast(TimestampType))

          val explodedDF: DataFrame = explodedDF1.withColumn("LogisticsInfo", explode($"shippingdata.LogisticsInfo"))

          val flattenedSchema: Array[Column] = DataframeTransformations.flattenSchema(explodedDF.schema, None)

          val renamedCols: Array[Column] = flattenedSchema.map(name => col(name.toString()).as(name.toString().replace(".", "_")))

          val outputDF: DataFrame = explodedDF.select(renamedCols: _*)

          val recentlyCreated: DataFrame = outputDF
            .filter(((col("origin") === 1) && (col("status") === "order-accepted") ) || ((col("origin") === 0) && (col("status") === "order-created")))

          val df2: DataFrame = recentlyCreated
            .withColumn("ordergroup", when(col("origin") === 1, col("orderid")).otherwise(col("ordergroup")))

          val selectedDf: DataFrame = df2
            .select(
              $"origin", $"orderid", $"ordergroup",
              $"LogisticsInfo_array_element_PickupStoreInfo_IsPickupStore",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode",
              $"hostname", $"value", $"totals_array_element_Id",
              $"totals_array_element_value"
            ).distinct()

          val agg_pickup_store_df: DataFrame = selectedDf
            .groupBy(
              $"origin", $"orderid", $"ordergroup",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode",
              $"hostname", $"value", $"totals_array_element_value",
              $"totals_array_element_Id"
            ).agg(max("LogisticsInfo_array_element_PickupStoreInfo_IsPickupStore").alias("isPickupStore"))

          val filtered_shipping_df: DataFrame = agg_pickup_store_df
            .filter(col("totals_array_element_Id") === "Shipping")
            .select(
              $"origin", $"orderid", $"ordergroup",
              $"storepreferencesdata_CountryCode",
              $"isPickupStore", $"storepreferencesdata_CurrencyCode",
              $"hostname", $"value", $"totals_array_element_value"
            )

          val final_df: DataFrame = filtered_shipping_df
            .groupBy(
              $"hostname", $"ordergroup", $"origin",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode"
            ).agg(
              sum("value").alias("value"),
              sum("totals_array_element_value").alias("totals_array_element_value"),
              max("isPickupStore").alias("isPickupStore")
            )

          val final_df2: DataFrame = final_df
            .withColumn(
              "is_free_shipping",
              when(col("totals_array_element_value") === 0, true)
                .otherwise(false))

          val final_df3: DataFrame = final_df2
            .groupBy(
              $"hostname", $"ordergroup", $"origin",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode"
            ).agg(
              count("ordergroup").alias("orders_number"),
              sum(col("is_free_shipping").cast("long")).alias("orders_free_shipping"),
              sum(col("isPickupStore").cast("long")).alias("orders_pickup"),
              sum("value").alias("revenue")
            )

          val dateTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(time.milliseconds),
                                    ZoneOffset.UTC)
          final_df3.write
            .mode(SaveMode.Append)
            .option("delimiter", ";")
            .option("header", true)
            .csv(outputDir +
              dateTime.format(date_formatter))
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

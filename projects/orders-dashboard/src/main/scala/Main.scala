import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}

import Helpers.DataframeTransformations
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object Main {

  private val date_formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm")

  def main(args: Array[String]): Unit = {

    implicit val sparkSession: SparkSession = SparkSession.builder
      .appName("BlackFridayDashboard")
      .getOrCreate()

    implicit val sc: SparkContext = sparkSession.sparkContext
    implicit val ssc: StreamingContext = new StreamingContext(sc, Seconds(60))

    ssc.sparkContext.setLogLevel("ERROR")

    val inputDir: String =
      "s3://vtex.datalake/raw_data/orders/parquet_stage/ingestion_year=2019/ingestion_month=12/ingestion_day=*/ingestion_hour=*/"

    val outputDir: String =
      "s3://vtex.datalake/test/BlackFridayDashboard/outputDir/"

    runSparkContext(inputDir: String, outputDir: String)
  }

  def runSparkContext(inputDir: String, outputDir: String)(
    implicit sparkSession: SparkSession,
    sc: SparkContext,
    ssc: StreamingContext): Unit = {

    // In order to properly read parquet data as a fileStream
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

    stream
      .map(_._2)
      .transform(rdd => rdd.map(record => record.toString))
      .foreachRDD((rdd: RDD[String], time: Time) => {

        import sparkSession.implicits.StringToColumn

        val dataDF: DataFrame = sparkSession.sqlContext.read.json(rdd)

        if (!dataDF.isEmpty) {
          val dfExplodedTotals: DataFrame = dataDF
            .withColumn("totals", explode($"totals"))

          val dfExplodedLogisticsInfo: DataFrame = dfExplodedTotals
            .withColumn("LogisticsInfo", explode($"shippingdata.LogisticsInfo"))

          val flattenedSchema: Array[Column] = DataframeTransformations
            .flattenSchema(schema = dfExplodedLogisticsInfo.schema, prefix = None)

          val renamedCols: Array[Column] = flattenedSchema.map(
            name =>
              col(name.toString())
                .as(name
                  .toString()
                  .replace(".", "_")))

          val flattenedDF: DataFrame = dfExplodedLogisticsInfo.select(renamedCols: _*)

          val recentlyCreated: DataFrame = flattenedDF
            .filter(
              ((col("origin") === 1) &&
                (col("status") === "order-accepted")) ||
              ((col("origin") === 0) &&
                (col("status") === "order-created"))
            )

          val orderGroupDF: DataFrame = recentlyCreated
            .withColumn("ordergroup",
              when(col("origin") === 1,
                col("orderid")
              ).otherwise(col("ordergroup")))

          val selectedDF: DataFrame = orderGroupDF
            .select(
              $"origin", $"orderid", $"ordergroup",
              $"LogisticsInfo_array_element_PickupStoreInfo_IsPickupStore",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode",
              $"hostname", $"value", $"totals_array_element_Id",
              $"totals_array_element_value"
            ).distinct()

          val aggByPickupStoreDF: DataFrame = selectedDF
            .groupBy(
              $"origin", $"orderid", $"ordergroup",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode",
              $"hostname", $"value", $"totals_array_element_value",
              $"totals_array_element_Id")
            .agg(max("LogisticsInfo_array_element_PickupStoreInfo_IsPickupStore").alias("isPickupStore"))

          val filteredByShippingDF: DataFrame = aggByPickupStoreDF
            .filter(col("totals_array_element_Id") === "Shipping")
            .select(
              $"origin", $"orderid", $"ordergroup",
              $"storepreferencesdata_CountryCode",
              $"isPickupStore", $"storepreferencesdata_CurrencyCode",
              $"hostname", $"value", $"totals_array_element_value")

          val totalOrderValueDF: DataFrame = filteredByShippingDF
            .groupBy(
              $"hostname", $"ordergroup", $"origin",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode"
            ).agg(
            sum("value").alias("value"),
            sum("totals_array_element_value").alias("totals_array_element_value"),
            max("isPickupStore").alias("isPickupStore")
          )

          val addFreeShippingColDF: DataFrame = totalOrderValueDF
            .withColumn("is_free_shipping",
              when(col("totals_array_element_value") === 0, true)
                .otherwise(false))

          val ordersCreated: DataFrame = addFreeShippingColDF
            .groupBy(
              $"hostname", $"origin",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode"
            ).agg(
            count("ordergroup").alias("orders_number"),
            sum(col("is_free_shipping").cast("long")).alias("orders_free_shipping"),
            sum(col("isPickupStore").cast("long")).alias("orders_pickup"),
            sum("value").alias("revenue")
          )

          val dateTime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(time.milliseconds),
            ZoneOffset.UTC)

          ordersCreated.write
            .mode(SaveMode.Append)
            .parquet(outputDir +
              dateTime.format(date_formatter))
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

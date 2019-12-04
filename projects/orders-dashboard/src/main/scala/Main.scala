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
  private val ColnameTotals = "totals"
  private val ColnameLogisticsInfo = "LogisticsInfo"
  private val ColnameOrigin = "origin"
  private val ColnameStatus = "status"
  private val ColnameOrderGroup = "ordergroup"
  private val ColnameOrderId = "orderid"
  private val ColnameIsPickUpStore = "LogisticsInfo_array_element_PickupStoreInfo_IsPickupStore"
  private val ColnameTotalsId = "totals_array_element_Id"
  private val CaolnameTotalsIdFlatten = "totals_id"
  private val ColnameTotalsValue = "totals_array_element_value"
  private val ColnameTotalsValueFlatten = "totals_value"
  private val ColnameValue = "value"
  private val ColnameRevenue = "revenue"
  private val ColnameIsFreeShipping = "is_free_shipping"
  private val ColnameOrdersFreeShipping = "orders_free_shipping"
  private val ColnameOrdersNumber = "orders_number"
  private val ColnameOrdersPickUp = "orders_pickup"
  private val ColnameCountryCode = "storepreferencesdata_CountryCode"
  private val ColnameCountryCodeFlatten = "country_code"
  private val ColnameCurrencyCode = "storepreferencesdata_CurrencyCode"
  private val ColnameCurrencyCodeFlatten = "currency_code"


  private val OrderAccepted = "order-accepted"
  private val IsPickUpStore = "is_pick_up_store"
  private val Shipping = "Shipping"



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
            .withColumn(ColnameTotals, explode($"totals"))

          val dfExplodedLogisticsInfo: DataFrame = dfExplodedTotals
            .withColumn(ColnameLogisticsInfo, explode($"shippingdata.LogisticsInfo"))

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
              ((col(ColnameOrigin) === 1) &&
                (col(ColnameStatus) === OrderAccepted)) ||
              ((col(ColnameOrigin) === 0) &&
                (col(ColnameStatus) === OrderAccepted))
            )

          val orderGroupDF: DataFrame = recentlyCreated
            .withColumn(ColnameOrderGroup,
              when(col(ColnameOrigin) === 1, col(ColnameOrderId))
                .otherwise(col(ColnameOrderGroup)))

          val selectedDF: DataFrame = orderGroupDF
            .select(
              $"origin",
              $"value",
              $"orderid",
              $"ordergroup",
              $"hostname",
              $"totals_array_element_Id",
              $"totals_array_element_value",
              $"LogisticsInfo_array_element_PickupStoreInfo_IsPickupStore",
              $"storepreferencesdata_CountryCode",
              $"storepreferencesdata_CurrencyCode"
            ).distinct()

          val renamedDF: DataFrame = selectedDF
            .withColumnRenamed(ColnameTotalsId, CaolnameTotalsIdFlatten)
            .withColumnRenamed(ColnameTotalsValue, ColnameTotalsValueFlatten)
            .withColumnRenamed(ColnameCountryCode, ColnameCountryCodeFlatten)
            .withColumnRenamed(ColnameCurrencyCode, ColnameCurrencyCodeFlatten)

          val aggByPickupStoreDF: DataFrame = renamedDF
            .groupBy(
              $"origin",
              $"value",
              $"orderid",
              $"hostname",
              $"ordergroup",
              $"totals_id",
              $"totals_value",
              $"country_code",
              $"currency_code")
            .agg(max(ColnameIsPickUpStore).alias(IsPickUpStore))

          val filteredByShippingDF: DataFrame = aggByPickupStoreDF
            .filter(col(CaolnameTotalsIdFlatten) === Shipping)
            .select(
              $"origin",
              $"value",
              $"orderid",
              $"hostname",
              $"ordergroup",
              $"is_pick_up_store",
              $"totals_value",
              $"country_code",
              $"currency_code")

          val totalOrderValueDF: DataFrame = filteredByShippingDF
            .groupBy(
              $"hostname",
              $"origin",
              $"ordergroup",
              $"country_code",
              $"currency_code"
            ).agg(
            sum(ColnameValue).alias(ColnameValue),
            sum(ColnameTotalsValueFlatten).alias(ColnameTotalsValueFlatten),
            max(IsPickUpStore).alias(IsPickUpStore)
          )

          val addFreeShippingColDF: DataFrame = totalOrderValueDF
            .withColumn(ColnameIsFreeShipping,
              when(col(ColnameTotalsValueFlatten) === 0, true)
                .otherwise(false))

          val ordersCreated: DataFrame = addFreeShippingColDF
            .groupBy(
              $"hostname",
              $"origin",
              $"country_code",
              $"currency_code"
            ).agg(
            count(ColnameOrderGroup).alias(ColnameOrdersNumber),
            sum(col(ColnameIsFreeShipping).cast("long")).alias(ColnameOrdersFreeShipping),
            sum(col(IsPickUpStore).cast("long")).alias(ColnameOrdersPickUp),
            sum(ColnameValue).alias(ColnameRevenue)
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

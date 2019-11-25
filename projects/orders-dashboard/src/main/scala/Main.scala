import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import Models.Order
import Helpers.DataframeTransformations

object Main {

  def main(args: Array[String]): Unit = {

    implicit val sparkSession: SparkSession = SparkSession.builder
//      .master("local[*]")
      .appName("BlackFridayDashboard")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    val inputDir: String =
      "s3://vtex.datalake/raw_data/orders/parquet_stage/ingestion_year=*/ingestion_month=*/ingestion_day=*/ingestion_hour=*/"

    val outputDir: String =
      "s3://vtex-ml/BlackFridayDashboard/Processed/"

    val checkpointDir: String =
      "s3://vtex-ml/BlackFridayDashboard/SparkCheckpoint/"

    runSparkContext(inputDir: String, outputDir: String, checkpointDir: String)
  }

  def runSparkContext(
      inputDir: String,
      outputDir: String,
      checkpointDir: String)(implicit sparkSession: SparkSession): Unit = {

    import sparkSession.implicits.StringToColumn

    val dfStream: DataFrame = sparkSession.readStream
      .schema(Order.Schema.schema)
      .option("latestFirst", "true")
      .parquet(inputDir)

    val explodedDF: DataFrame = dfStream
      .withColumn("totals", explode($"totals"))
      .withColumn("creationdate", $"creationdate".cast(TimestampType))

    val flattenedSchema: Array[Column] =
      DataframeTransformations.flattenSchema(explodedDF.schema, None)

    val renamedCols: Array[Column] = flattenedSchema.map(name =>
      col(name.toString()).as(name.toString().replace(".", "_")))

    val outputDF: DataFrame = explodedDF.select(renamedCols: _*)

    val query: StreamingQuery = outputDF
      .select(hour($"creationdate") as "hour",
              dayofmonth($"creationdate") as "day",
              month($"creationdate") as "month",
              year($"creationdate") as "year",
              $"*")
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .outputMode("append")
      .format("parquet")
      .option("path", outputDir)
      .option("checkpointLocation", checkpointDir)
      .start()

    query.awaitTermination()
  }
}

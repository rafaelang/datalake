import Models.Order
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main2 {

  def main(args: Array[String]): Unit = {

    implicit val sparkSession: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("BlackFridayDashboard")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    val inputDir: String =
      "/Users/diego/repos/datalake/projects/orders-dashboard/inputDir/"

    val outputDir: String =
      "/Users/diego/repos/datalake/projects/orders-dashboard/outputDir"

    val checkpointDir: String =
      "/Users/diego/repos/datalake/projects/orders-dashboard/checkpoint"

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

//    val explodedDF: DataFrame = dfStream
//      .withColumn("totals", explode($"totals"))
//      .withColumn("creationdate", $"creationdate".cast(TimestampType))
//
//    val flattenedSchema: Array[Column] =
//      DataframeTransformations.flattenSchema(explodedDF.schema, None)
//
//    val renamedCols: Array[Column] = flattenedSchema.map(name =>
//      col(name.toString()).as(name.toString().replace(".", "_")))
//
//    val outputDF: DataFrame = explodedDF.select(renamedCols: _*)

    val query: StreamingQuery = dfStream
      .select(hour($"creationdate")  as "hour",
              dayofmonth($"creationdate") as "day",
              month($"creationdate") as "month",
              year($"creationdate") as "year",
              $"*")
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

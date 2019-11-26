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
    implicit val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    val inputDir: String =
      "/Users/diego/repos/datalake/projects/orders-dashboard/inputDir/"

    val outputDir: String =
      "/Users/diego/repos/datalake/projects/orders-dashboard/outputDir/"

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

          // TODO: Select the columns before or force the schema
          val explodedDF = dataDF
            .withColumn("totals", explode($"totals"))
            .withColumn("creationdate", $"creationdate".cast(TimestampType))

          // TODO: Transformations here

          val flattenedSchema: Array[Column] =
            DataframeTransformations.flattenSchema(explodedDF.schema, None)

          val renamedCols: Array[Column] = flattenedSchema.map(
            name =>
              col(name.toString())
                .as(name.toString().replace(".", "_")))

          val outputDF: DataFrame = explodedDF.select(renamedCols: _*)

          val dateTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(time.milliseconds),
                                    ZoneOffset.UTC)
          outputDF.write
            .mode(SaveMode.Append)
            .parquet(outputDir +
              dateTime.format(date_formatter))
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

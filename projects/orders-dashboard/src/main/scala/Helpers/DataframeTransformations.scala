package Helpers

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

object DataframeTransformations {
  def flattenSchema(schema: StructType,
                    prefix: Option[String]): Array[Column] = {
    schema.fields.flatMap(f => {

      val colName = prefix.fold(f.name)(_ + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, Some(colName))
        case _              => Array(col(colName))
      }
    })
  }
}

package Models

import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

object Order {
  object Schema {
    val totals = StructField(
      "totals",
      ArrayType(
        StructType(
          Array(
            StructField("Id", StringType, nullable = true),
            StructField("Name", StringType, nullable = true),
            StructField("value", IntegerType, nullable = true)
          )
        )
      )
    )

    val shippingdata = StructField(
      "shippingdata",
      StructType(
        Array(
          StructField(
            "LogisticsInfo",
            ArrayType(
              StructType(
                Array(
                  StructField("PickupStoreInfo",
                              StructType(
                                Array(
                                  StructField("IsPickupStore",
                                              BooleanType,
                                              nullable = true)
                                )
                              ))
                )
              )
            ),
            nullable = true
          )
        )
      )
    )

    val storepreferencesdata = StructField(
      "storepreferencesdata",
      StructType(
        Array(
          StructField("CountryCode", StringType, nullable = true),
          StructField("CurrencyCode", StringType, nullable = true)
        )
      ))

    val schema = StructType(
      Array(
        StructField("status", StringType, nullable = true),
        StructField("origin", IntegerType, nullable = true),
        StructField("ordergroup", StringType, nullable = true),
        StructField("orderid", StringType, nullable = true),
        StructField("hostname", StringType, nullable = true),
        StructField("creationdate", StringType, nullable = true),
        StructField("value", IntegerType, nullable = true),
        StructField("iscompleted", BooleanType, nullable = true),
        totals,
        shippingdata,
        storepreferencesdata
      ))
  }
}

package sh.ctr.ml.xgboost.data

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object GbmSchema {

  val CTR_SCHEMA = new StructType(Array(

    StructField("os", StringType, nullable = true),
    StructField("nets", StringType, nullable = true)))

  val CTR_CATEGORIES: Set[String] = Set(
    "os",
    "nets"
  )
}

package com.pawdon.growth

import org.apache.spark.sql.SparkSession

object MainApp extends App {
  val spark = SparkSession.builder.master("local").appName("My App").getOrCreate()
  RddRelation.capitolToCountryDensityRatio(spark)
}

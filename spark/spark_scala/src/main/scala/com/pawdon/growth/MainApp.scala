package com.pawdon.growth

import org.apache.spark.sql.SparkSession

import com.pawdon.growth._

object MainApp extends App {
//  val spark = SparkSession.builder.master("local").appName("My App").getOrCreate()
  RddRelation.convert
}

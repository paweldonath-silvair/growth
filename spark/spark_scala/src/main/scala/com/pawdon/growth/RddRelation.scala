package com.pawdon.growth

import org.apache.spark.sql.SparkSession

import scala.util.{Try, Failure, Success}
import scala.io.Source

import com.pawdon.growth._

object RddRelation {
  def test01(spark: SparkSession): Unit = {
    val data = spark.sparkContext.parallelize(Seq(1, 3, 5))
    val result = data
      .filter(_ > 2)
      .map(_ * 7)
    println(result.collect().toList)
  }

  def convert: Unit = {
    val data = JsonClassUtils.readCities
    println(data)
  }
}

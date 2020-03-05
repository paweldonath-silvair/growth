package com.pawdon.growth

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object RddNested {
  def round(v: Double, d: Int): Double = {
    val x = Math.pow(10.0, d)
    Math.round(v * x) * 1.0 / x
  }

  def showAll[A](rdd: RDD[A]): Unit = println(rdd.collect().toList)

  def showSeparately[A](rdd: RDD[A]): Unit = rdd.foreach(x => println(x))

  def convert(spark: SparkSession): Unit = {
    val data = JsonClassUtils.readCountriesFull.get
    println(data)
    val rdd = spark.sparkContext.parallelize(data)
    showAll(rdd)
    showSeparately(rdd)
  }

  def multipleCountryMeanCityDensity(spark: SparkSession): Unit = {
    val countries = spark.sparkContext.parallelize(JsonClassUtils.readCountriesFull.get)

    val result = countries
        .map(x => (x.name, x.cities))
        .mapValues(_.map(x => x.population / x.area))
        .mapValues(x => x.sum / x.length)

    showSeparately(result)
  }

  def capitolToCountryDensityRatio(spark: SparkSession): Unit = {
    val countries = spark.sparkContext.parallelize(JsonClassUtils.readCountriesFull.get)

    val result = countries
      .filter(_.capitol.nonEmpty)
      .map(x => (x.name, x.capitol.get.name, (x.capitol.get.population / x.capitol.get.area) / (x.population / x.area)))

    showSeparately(result)
  }

}

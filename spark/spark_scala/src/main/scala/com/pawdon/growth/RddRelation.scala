package com.pawdon.growth

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

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

  def round(v: Double, d: Int): Double = {
    val x = Math.pow(10.0, d)
    Math.round(v * x) * 1.0 / x
  }

  def showAll[A](rdd: RDD[A]): Unit = println(rdd.collect().toList)

  def showSeparately[A](rdd: RDD[A]): Unit = rdd.foreach(x => println(x))

  def convert(spark: SparkSession): Unit = {
    val data = JsonClassUtils.readCities.get
    println(data)
    val rdd = spark.sparkContext.parallelize(data)
    showAll(rdd)
    showSeparately(rdd)
  }

  def multipleCountryMeanCityDensity(spark: SparkSession): Unit = {
    val cities = spark.sparkContext.parallelize(JsonClassUtils.readCities.get)
    val countries = spark.sparkContext.parallelize(JsonClassUtils.readCountries.get)

    val isoTranslator = countries.map(x => (x.iso, x.name)).collectAsMap()

    val result = cities
      .filter(x => isoTranslator.contains(x.country))
      .map(x => (isoTranslator(x.country), round(x.population * 1.0 / x.area, 2)))
        .aggregateByKey(zeroValue = (0.0, 0))(
          seqOp = (agg, x) => (agg._1 + x, agg._2 + 1),
          combOp = (agg1, agg2) => (agg1._1 + agg2._1, agg1._2 + agg2._2)
        )
        .mapValues { case(sum, count) => round(sum / count, 2) }

    showSeparately(result)
  }

  def capitolToCountryDensityRatio(spark: SparkSession): Unit = {
    val cities = spark.sparkContext.parallelize(JsonClassUtils.readCities.get)
    val countries = spark.sparkContext.parallelize(JsonClassUtils.readCountries.get)

    val citiesMap = cities.map(x => (x.name, x))
    val result = countries
      .map(x => (x.capitol, x))
      .join(citiesMap)
      .map { case(_, (country, capitol)) => Map(
        "country_name" -> country.name,
        "capitol_name" -> capitol.name,
        "country_density" -> round(country.population * 1.0 / country.area, 2),
        "capitol_density" -> round(capitol.population * 1.0 / capitol.area, 2),
        "density_ratio" -> round((capitol.population * 1.0 / capitol.area) / (country.population * 1.0 / country.area), 2)
      ) }

    showSeparately(result)
  }
}

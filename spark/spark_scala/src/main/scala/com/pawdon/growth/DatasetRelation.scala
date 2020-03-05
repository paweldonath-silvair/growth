package com.pawdon.growth

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, TypedColumn, functions => F}
import org.apache.spark.sql.expressions.scalalang.typed

object DatasetRelation {
  def showAll[A](ds: Dataset[A]): Unit = {
    println(ds.getClass)
    ds.printSchema()
    ds.show()
  }

  def convert01(spark: SparkSession): Unit = {
    import spark.implicits._
    val data = JsonClassUtils.readCities.get
    println(data)
    val ds = spark.sqlContext.createDataFrame(data).as[City]
    showAll(ds)
  }

  def convert02(spark: SparkSession): Unit = {
    import spark.implicits._
    val data = JsonClassUtils.readCities.get
    println(data)
    val ds = spark.sqlContext.createDataset(data)
    showAll(ds)
  }

  def round(v: Double, d: Int): Double = {
    val x = Math.pow(10.0, d)
    Math.round(v * x) * 1.0 / x
  }

  def multipleCountryMeanCityDensity01(spark: SparkSession): Unit = {
    import spark.implicits._
    val cities = spark.sqlContext.createDataset(JsonClassUtils.readCities.get)
    val countries = JsonClassUtils.readCountries.get

    val isoTranslator = countries.map(x => (x.iso, x.name)).toMap
    val translator = F.udf(isoTranslator(_))

    val result = cities
      .filter(F.col("country").isin(isoTranslator.keys.toSeq:_*))
      .withColumn("country_name", translator(F.col("country")))
      .withColumn("density", F.round(F.col("population") / F.col("area"), 2))
      .groupBy("country_name")
      .agg(Map("density" -> "mean"))
      .withColumn("mean_density", F.round(F.col("avg(density)"), 2))


    showAll(result)
  }

  def multipleCountryMeanCityDensity02(spark: SparkSession): Unit = {
    import spark.implicits._
    val cities = spark.sqlContext.createDataset(JsonClassUtils.readCities.get)
    val countries = JsonClassUtils.readCountries.get

    val isoTranslator = countries.map(x => (x.iso, x.name)).toMap

    // groupByKey is an experimental API
    val result = cities
      .filter(x => isoTranslator.contains(x.country))
      .map(x => CityDensity(isoTranslator(x.country), round(x.population * 1.0 / x.area, 2)))
      .groupByKey(_.countryName)
      .agg(typed.avg(_.density))
      .map(x => CityDensity(x._1, round(x._2, 2)))

    showAll(result)
  }

//  def capitolToCountryDensityRatio(spark: SparkSession): Unit = {
//    val cities = spark.sparkContext.parallelize(JsonClassUtils.readCities.get)
//    val countries = spark.sparkContext.parallelize(JsonClassUtils.readCountries.get)
//
//    val citiesMap = cities.map(x => (x.name, x))
//    val result = countries
//      .map(x => (x.capitol, x))
//      .join(citiesMap)
//      .map { case(_, (country, capitol)) => Map(
//        "country_name" -> country.name,
//        "capitol_name" -> capitol.name,
//        "country_density" -> round(country.population * 1.0 / country.area, 2),
//        "capitol_density" -> round(capitol.population * 1.0 / capitol.area, 2),
//        "density_ratio" -> round((capitol.population * 1.0 / capitol.area) / (country.population * 1.0 / country.area), 2)
//      ) }
//
//    showSeparately(result)
//  }

}

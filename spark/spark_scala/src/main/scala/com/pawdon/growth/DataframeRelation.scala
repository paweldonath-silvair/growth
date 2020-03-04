package com.pawdon.growth

import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}


object DataframeRelation {
  def showAll(df: DataFrame): Unit = {
    df.printSchema()
    df.show()
  }

  def convert(spark: SparkSession): Unit = {
    val data = JsonClassUtils.readCities.get
    println(data)
    val df = spark.sqlContext.createDataFrame(data)
    showAll(df)
  }

  def multipleCountryMeanCityDensity(spark: SparkSession): Unit = {
    val cities = spark.sqlContext.createDataFrame(JsonClassUtils.readCities.get)
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

  def capitolToCountryDensityRatio(spark: SparkSession): Unit = {
    val cities = spark.sqlContext.createDataFrame(JsonClassUtils.readCities.get)
    val countries = spark.sqlContext.createDataFrame(JsonClassUtils.readCountries.get)

    cities.createTempView("cities")
    countries.createTempView("countries")

    spark.sql("SELECT " +
      "countries.name AS country_name, " +
      "cities.name AS capitol_name, " +
      "round(countries.population / countries.area, 2) AS country_density, " +
      "round(cities.population / cities.area, 2) AS capitol_density " +
      "FROM countries JOIN cities on countries.capitol == cities.name")
      .createTempView("country_capitol")

    val result = spark.sql("SELECT *, " +
      "round(capitol_density / country_density, 2) AS density_ratio " +
      "FROM country_capitol")

    showAll(result)
  }

}

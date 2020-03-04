package com.pawdon.growth

import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}

object DataframeNested {
  def showAll(df: DataFrame): Unit = {
    df.printSchema()
    df.show()
  }

  def convert(spark: SparkSession): Unit = {
    val data = JsonClassUtils.readCountriesFull.get
    println(data)
    val df = spark.sqlContext.createDataFrame(data)
    showAll(df)
  }

  def multipleCountryMeanCityDensity(spark: SparkSession): Unit = {
    val countries = spark.sqlContext.createDataFrame(JsonClassUtils.readCountriesFull.get)
    countries.createTempView("countries")

    val subResultColumn = "cities_density_list"

    spark.sqlContext.sql(sqlText =
      s"SELECT " +
        s"name, " +
        s"TRANSFORM(cities, c -> round(c.population / c.area, 2)) AS $subResultColumn " +
        s"FROM countries"
    ).createTempView("countries_cities_density")

    val result = spark.sqlContext.sql(sqlText =
      s"SELECT *, " +
        s"aggregate(`$subResultColumn`, " +
        s"CAST(0.0 AS double), " +
        s"(acc, x) -> acc + x," +
        s"acc -> round(acc / size(`$subResultColumn`), 2)" +
        s") AS  `avg_$subResultColumn`" +
        s"FROM countries_cities_density"
    )

    showAll(result)
  }

  def capitolToCountryDensityRatio(spark: SparkSession): Unit = {
    val countries = spark.sqlContext.createDataFrame(JsonClassUtils.readCountriesFull.get)
    countries.createTempView("countries")

    spark.sqlContext.sql(sqlText =
      "SELECT " +
        "name AS country_name, " +
        "capitol.name AS capitol_name, " +
        "round(population / area, 2) AS country_density, " +
        "round(capitol.population / capitol.area, 2) AS capitol_density " +
        "FROM countries " +
        "WHERE capitol IS NOT NULL"
    ).createTempView("country_capitol")

    val result = spark.sqlContext.sql(sqlText =
      "SELECT *, " +
        "round(capitol_density / country_density, 2) AS density_ratio " +
        "FROM country_capitol"
    )

    showAll(result)
  }

}

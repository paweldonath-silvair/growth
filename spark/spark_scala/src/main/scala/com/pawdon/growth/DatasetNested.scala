package com.pawdon.growth

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, TypedColumn, functions => F}
import org.apache.spark.sql.expressions.scalalang.typed

object DatasetNested {
  def showAll[A](ds: Dataset[A]): Unit = {
    println(ds.getClass)
    ds.printSchema()
    ds.show()
  }

  def convert(spark: SparkSession): Unit = {
    import spark.implicits._
    val data = JsonClassUtils.readCountriesFull.get
    println(data)
    val ds = spark.sqlContext.createDataFrame(data).as[CountryFull]
    showAll(ds)
  }

  def multipleCountryMeanCityDensity(spark: SparkSession): Unit = {
    import spark.implicits._
    val countries = spark.sqlContext.createDataset(JsonClassUtils.readCountriesFull.get)

    val result = countries
      .map(x => (x.name, x.cities))
      .map(x => (x._1, x._2.map(y => y.population / y.area)))
      .map(x => (x._1, x._2.sum / x._2.length))

    showAll(result)
  }

}

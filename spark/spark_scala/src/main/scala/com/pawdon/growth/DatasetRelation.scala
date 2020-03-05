package com.pawdon.growth

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}

object DatasetRelation {
  def showAll[A](ds: Dataset[A]): Unit = {
    println(ds)
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

}

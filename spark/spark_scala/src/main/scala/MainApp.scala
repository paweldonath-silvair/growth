import org.apache.spark.sql.SparkSession

object MainApp extends App {
  val spark = SparkSession.builder
    .master("local")
    .appName("Word count")
    .getOrCreate()
  val data = spark.sparkContext.parallelize(Seq(1, 3, 5))
  val result = data
      .filter(_ > 2)
      .map(_ * 7)
  println(result.collect().toList)
}

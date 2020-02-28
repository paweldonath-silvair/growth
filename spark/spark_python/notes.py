import json
import socket

import pandas as pd
from pyspark import SparkContext, TaskContext, RDD
from pyspark.sql import SQLContext, DataFrame, Row, functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, ArrayType, MapType, BooleanType


def task_info(*_):
    ctx = TaskContext()
    return ["Stage: {0}, Partition: {1}, Host: {2}".format(
        ctx.stageId(), ctx.partitionId(), socket.gethostname())]


def test01(sc: SparkContext):
    log_file = "SPARK_README.md"
    log_data = sc.textFile(log_file).cache()
    num_as = log_data.filter(lambda s: 'a' in s).count()
    num_bs = log_data.filter(lambda s: 'b' in s).count()
    print("Lines with a: %i, lines with b: %i" % (num_as, num_bs))


def test02(sc: SparkContext):
    words = sc.parallelize([
        "scala",
        "java",
        "hadoop",
        "spark",
        "akka",
        "spark vs hadoop",
        "pyspark",
        "pyspark and spark"
    ])
    print("words", type(words), words)
    counts = words.count()
    print("counts", type(counts), counts)
    coll = words.collect()
    print(coll)
    fore = words.foreach(lambda x: print(type(x), x, task_info()))
    filtered = words.filter(lambda x: 'spark' in x).collect()
    input()


def rdd_test(sc: SparkContext):
    with open("cities.json") as f:
        py_data = json.load(f)
    rdd = sc.parallelize(py_data)
    print(rdd.collect())


def rdd_group(sc: SparkContext):
    with open("cities.json") as f:
        py_data = json.load(f)
    rdd = sc.parallelize(py_data)
    print(rdd.collect())
    rdd = rdd.groupBy(lambda x: x['country'])
    print(rdd.collect())
    rdd = rdd.map(lambda x: {'country': x[0], 'cities': list(x[1])})
    print(rdd.collect())
    rdd.foreach(lambda x: print(x['country'], x['cities']))

    py_data = rdd.collect()
    with open("countries.json", "w") as f:
        json.dump(py_data, f)


def dataframe_group(sc: SparkContext):
    sqlContext = SQLContext(sc)
    with open("cities.json") as f:
        py_data = json.load(f)
    pdf = pd.DataFrame(py_data)
    df = sqlContext.createDataFrame(pdf)
    df.printSchema()
    df.show()
    df = df.groupBy('country')
    df.printSchema()
    df.show()


def dataframe_read_grouped_simple_test(sc: SparkContext):
    sqlContext = SQLContext(sc)
    py_data = {
        "ErrorMessage": None,
        "IsError": False,
        "Report": {
            "tl": [
                {
                    "TlID": "F6",
                    "CID": "mo"
                },
                {
                    "TlID": "Fk",
                    "CID": "mo"
                }
            ]
        }
    }
    print(py_data)

    inner_schema = StructType([
        StructField("CID", StringType()),
        StructField("TlID", StringType())
    ])

    report_schema = StructType([
        StructField("tl", ArrayType(inner_schema))
    ])

    schema = StructType([
        StructField("ErrorMessage", StructType()),
        StructField("IsError", BooleanType()),
        StructField("Report", report_schema)
    ])

    rdd = sc.parallelize([py_data])
    print(rdd.collect())
    rdd_row = rdd.map(lambda x: Row(**x))
    df = sqlContext.createDataFrame(rdd_row, schema)
    df.printSchema()
    df.show()


def dataframe_read_grouped_simple_test2(sc: SparkContext):
    sqlContext = SQLContext(sc)
    py_data = {
        "ErrorMessage": None,
        "IsError": False,
        "Report": [
            {
                "TlID": "F6",
                "CID": "mo"
            },
            {
                "TlID": "Fk",
                "CID": "mo"
            }
        ]
    }
    print(py_data)

    inner_schema = StructType([
        StructField("CID", StringType()),
        StructField("TlID", StringType())
    ])

    schema = StructType([
        StructField("ErrorMessage", StructType()),
        StructField("IsError", BooleanType()),
        StructField("Report", ArrayType(inner_schema))
    ])

    rdd = sc.parallelize([py_data, py_data])
    print(rdd.collect())
    rdd_row = rdd.map(lambda x: Row(**x))
    df = sqlContext.createDataFrame(rdd_row, schema)
    df.printSchema()
    df.show()


def dataframe_read_grouped_simple(sc: SparkContext):
    sqlContext = SQLContext(sc)
    with open("countries_simple.json") as f:
        py_data = json.load(f)
    rdd = sc.parallelize(py_data)
    print(rdd.collect())
    rdd_row = rdd.map(lambda x: Row(**x))
    country_schema = StructType([
        StructField('country', StringType()),
        StructField('cities', ArrayType(StringType())),
    ])
    df = sqlContext.createDataFrame(rdd_row, country_schema)
    df.printSchema()
    df.show()


def dataframe_read_grouped_simple2(sc: SparkContext):
    sqlContext = SQLContext(sc)
    with open("countries_simple2.json") as f:
        py_data = json.load(f)

    country_key = "animal"
    py_data = {
        country_key: "Spain",
        "cities": [
            {"name": "Madrid"},
            {"name": "Barcelona"}
        ]
    }
    print(py_data)

    inner_schema = StructType([
        StructField("name", StringType())
    ])

    schema = StructType([
        StructField(country_key, StringType()),
        StructField("cities", ArrayType(inner_schema))
    ])

    rdd = sc.parallelize([py_data, py_data])
    print(rdd.collect())
    rdd_row = rdd.map(lambda x: Row(**x))
    df = sqlContext.createDataFrame(rdd_row, schema)
    df.printSchema()
    df.show()


def dataframe_read_grouped(sc: SparkContext):
    sqlContext = SQLContext(sc)
    with open("countries.json") as f:
        py_data = json.load(f)
    rdd = sc.parallelize(py_data)
    print(rdd.collect())
    rdd_row = rdd.map(lambda x: Row(**x))
    df = sqlContext.createDataFrame(rdd_row)
    df.printSchema()
    df.show()

    city_schema = StructType([
        StructField('name', StringType()),
        #StructField('country', StringType()),
        #StructField('area', DoubleType()),
        #StructField('population', IntegerType()),
        #StructField('capitol', StringType())
    ])
    country_schema = StructType([
        StructField('country', StringType()),
        StructField('cities', ArrayType(StructType([
                        StructField('name', StringType())
        ]))),
    ])
    df = sqlContext.createDataFrame(rdd_row, country_schema)
    df.printSchema()
    df.show()


def dataframe_pd_test(sc: SparkContext):
    sqlContext = SQLContext(sc)
    with open("cities.json") as f:
        py_data = json.load(f)
        print(py_data)
    pdf = pd.DataFrame(py_data)
    print(pdf)
    df = sqlContext.createDataFrame(pdf)
    df.show()
    df.printSchema()


def dataframe_test(sc: SparkContext):
    sqlContext = SQLContext(sc)
    with open("cities.json") as f:
        py_data = json.load(f)
        print(py_data)
    rdd = sc.parallelize(py_data)
    print(rdd.collect())
    rdd_row = rdd.map(lambda x: Row(**x))
    print(rdd_row.collect())
    schema = StructType([
        StructField('name', StringType(), nullable=False),
        StructField('country', StringType(), nullable=False),
        StructField('area', DoubleType(), nullable=False),
        # StructField('population', IntegerType(), nullable=False),  # not all fields from dict must be included
        StructField('capitol', StringType(), nullable=True)
    ])
    # df = rdd_row.toDF()
    # df = sqlContext.createDataFrame(rdd_row)
    # df = sqlContext.createDataFrame(rdd_row, ['name', 'country', 'area', 'population']) PROBLEM WITH ARGUMENTS ORDER
    # df = sqlContext.createDataFrame(rdd_row, ['name', 'country', 'area', 'population', 'xxx']) NOT WORK
    df = sqlContext.createDataFrame(rdd_row, schema)
    df.show()
    df.select(['name', 'area']).show()
    df.filter(df.area > 400).show()
    df.filter(F.col('area') < 400).show()
    df.withColumn('area_m2', F.col('area') * 10**6).show()
    df.registerTempTable('some_table')
    sqlContext.sql('SELECT name, area * 1000000 AS area_m2 FROM some_table').show()
    df.rdd.map(lambda x: Row(name=x['name'], country=x['country'].upper())).toDF().show()
    df.withColumn('country_small', F.udf(lambda x: x.lower()[:3], StringType())('country')).show()


def main(app_name="first app"):
    print("SPARK_START")
    sc = SparkContext("local", app_name)
    print("MAIN_START")
    dataframe_read_grouped_simple(sc)
    print("MAIN_STOP")


if __name__ == "__main__":
    main()

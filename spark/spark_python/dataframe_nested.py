from typing import Dict, Any

import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext, DataFrame, Row, functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, ArrayType, MapType, BooleanType

import class_utils


def show_all(df: DataFrame):
    df.printSchema()
    df.show()


def multiple_country_mean_city_density(sc: SparkContext):
    sql_sc = SQLContext(sc)

    city_schema = StructType([
        StructField('name', StringType()),
        StructField('country', StringType()),
        StructField('area', DoubleType()),
        StructField('population', IntegerType())
    ])
    country_schema = StructType([
        StructField('name', StringType()),
        StructField('iso', StringType()),
        StructField('calling_code', StringType()),
        StructField('area', DoubleType()),
        StructField('population', IntegerType()),
        StructField('capitol', city_schema),
        StructField('cities', ArrayType(city_schema))
    ])

    countries = sql_sc.createDataFrame(pd.DataFrame(class_utils.read_countries_full()), country_schema)  # work without schema too

    countries.registerTempTable('countries')

    result = sql_sc.sql('SELECT '
                        'name, '
                        'TRANSFORM(cities, c -> round(c.population / c.area, 2)) AS cities_density_list '
                        'FROM countries')#.registerTempTable('countries_with_cities_density_list')

    query = """aggregate(
        `{col}`,
        CAST(0.0 AS double),
        (acc, x) -> acc + x,
        acc -> round(acc / size(`{col}`), 2)
    ) AS  `avg_{col}`""".format(col="cities_density_list")

    result = result.selectExpr("*", query)

    show_all(result)


def capitol_to_country_density_ratio(sc: SparkContext):
    sql_sc = SQLContext(sc)

    city_schema = StructType([
        StructField('name', StringType()),
        StructField('country', StringType()),
        StructField('area', DoubleType()),
        StructField('population', IntegerType())
    ])
    country_schema = StructType([
        StructField('name', StringType()),
        StructField('iso', StringType()),
        StructField('calling_code', StringType()),
        StructField('area', DoubleType()),
        StructField('population', IntegerType()),
        StructField('capitol', city_schema),
        StructField('cities', ArrayType(city_schema))
    ])

    countries = sql_sc.createDataFrame(pd.DataFrame(class_utils.read_countries_full()), country_schema)  # work without schema too

    countries.registerTempTable('countries')

    sql_sc.sql('SELECT '
               'name AS country_name, '
               'capitol.name AS capitol_name, '
               'round(population / area, 2) AS country_density, '
               'round(capitol.population / capitol.area, 2) AS capitol_density '
               'FROM countries '
               'WHERE capitol IS NOT NULL').registerTempTable('country_capitol')

    result = sql_sc.sql('SELECT *, '
                        'round(capitol_density / country_density, 2) AS density_ratio '
                        'FROM country_capitol')

    show_all(result)

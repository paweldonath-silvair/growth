from typing import Dict, Any

import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext, DataFrame, Row, functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, ArrayType, MapType, BooleanType

import class_utils


def show_all(df: DataFrame):
    df.printSchema()
    df.show()


def convert_01(sc: SparkContext):
    sql_sc = SQLContext(sc)

    cities = class_utils.read_cities()
    rdd = sc.parallelize(cities)
    rdd_row = rdd.map(lambda x: Row(**x))  # Using RDD of dict to inferSchema is deprecated

    df = sql_sc.createDataFrame(rdd_row)
    show_all(df)


def convert_02(sc: SparkContext):
    sql_sc = SQLContext(sc)

    cities = class_utils.read_cities()
    cities_row = [Row(**x) for x in cities]

    df = sql_sc.createDataFrame(cities_row)
    show_all(df)


def convert_03(sc: SparkContext):
    sql_sc = SQLContext(sc)

    cities = class_utils.read_cities()
    cities = [class_utils.City.from_dict(x) for x in cities]

    df = sql_sc.createDataFrame(cities)
    show_all(df)


def convert_04(sc: SparkContext):
    sql_sc = SQLContext(sc)

    cities = class_utils.read_cities()
    pd_df = pd.DataFrame(cities)

    df = sql_sc.createDataFrame(pd_df)
    show_all(df)


def convert_05(sc: SparkContext):
    sql_sc = SQLContext(sc)

    cities = class_utils.read_cities()
    cities_row = [Row(**x) for x in cities]

    schema = StructType([
        StructField('name', StringType(), nullable=False),
        StructField('country', StringType(), nullable=False),
        StructField('area', DoubleType(), nullable=False),
        # StructField('population', IntegerType(), nullable=False)
    ])

    df = sql_sc.createDataFrame(cities_row, schema)
    show_all(df)


def multiple_country_mean_city_density(sc: SparkContext):
    sql_sc = SQLContext(sc)

    countries = class_utils.read_countries()
    cities = sql_sc.createDataFrame(pd.DataFrame(class_utils.read_cities()))

    iso_translator = {x['iso']: x['name'] for x in countries}

    translator_udf = F.udf(lambda x: iso_translator.get(x), StringType())

    result = cities\
        .filter(F.col('country').isin(list(iso_translator.keys())))\
        .withColumn('country_name', translator_udf(F.col('country')))\
        .withColumn('density', F.col('population') / F.col('area'))\
        .groupBy('country_name')\
        .agg({'density': 'mean'})\
        .withColumn('mean_density', F.round(F.col('avg(density)'), 2))

    show_all(result)


def capitol_to_country_density_ratio(sc: SparkContext):
    sql_sc = SQLContext(sc)

    countries = sql_sc.createDataFrame(pd.DataFrame(class_utils.read_countries()))
    cities = sql_sc.createDataFrame(pd.DataFrame(class_utils.read_cities()))

    countries.registerTempTable('countries')
    cities.registerTempTable('cities')

    sql_sc.sql('SELECT '
               'countries.name AS country_name, '
               'cities.name AS capitol_name, '
               'round(countries.population / countries.area, 2) AS country_density, '
               'round(cities.population / cities.area, 2) AS capitol_density '
               'FROM countries JOIN cities on countries.capitol == cities.name').registerTempTable('country_capitol')

    result = sql_sc.sql('SELECT *, '
                        'round(capitol_density / country_density, 2) AS density_ratio '
                        'FROM country_capitol')

    show_all(result)

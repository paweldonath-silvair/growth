import json
import yaml

from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructField, StructType, DoubleType

from others.influx_reader import get_data


def test01(sc):
    words = sc.parallelize(
        ["scala",
         "java",
         "hadoop",
         "spark",
         "akka",
         "spark vs hadoop",
         "pyspark",
         "pyspark and spark"]
    )
    coll = words.collect()
    print(coll)


def print_type_value(x, *args):
    print(type(x), x, *args)


def print_yaml(x):
    print(f'len = {len(x)}')
    print(yaml.dump(x))


def simple_test_rdd(sc: SparkContext):
    py_data = read_data("data/energy_agg_test.json")
    rdd = sc.parallelize(py_data)
    print_type_value(rdd)
    print_yaml(rdd.collect())
    print()
    rdd2 = rdd.filter(lambda x: x['network_id'] == '491b1ee2611841a99576c4d03ca4c8d4')


    print_yaml(rdd2.collect())


def simple_test_dataframe(sc: SparkContext):
    py_data = read_data("data/energy_agg_test.json")
    rdd = sc.parallelize(py_data)
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField('energy', DoubleType(), nullable=False),
        StructField('energy', DoubleType(), nullable=False),
        StructField('energy', DoubleType(), nullable=False),
        StructField('energy', DoubleType(), nullable=False),
        StructField('energy', DoubleType(), nullable=False),
        StructField('energy', DoubleType(), nullable=False),
        StructField('energy', DoubleType(), nullable=False)
    ])

    df = sqlContext.createDataFrame(py_data)
    print_type_value(df)


def main():
    print("MAIN_START")
    sc = SparkContext("local", "My test app")
    simple_test_dataframe(sc)
    print("MAIN_STOP")


def read_data(filename):
    with open(filename) as f:
        data = json.load(f)
    return data


def save_data():
    filename = "data/energy_agg_test.json"
    data = get_data("dev_agg", "energy_agg", limit=10)
    with open(filename, 'w') as f:
        json.dump(data, f)


if __name__ == "__main__":
    main()

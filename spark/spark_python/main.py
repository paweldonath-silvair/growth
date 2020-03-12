from pyspark import SparkContext

import rdd_relation
import dataframe_relation
import dataframe_nested


def main(app_name="first app"):
    print("SPARK_START")
    sc = SparkContext("local", app_name)
    print("MAIN_START")
    dataframe_nested.multiple_country_mean_city_density(sc)
    print("MAIN_STOP")


if __name__ == "__main__":
    main()

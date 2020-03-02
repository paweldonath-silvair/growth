from pyspark import SparkContext

import rdd_relation
import dataframe_relation
import dataframe_nested


def main(app_name="first app"):
    print("SPARK_START")
    sc = SparkContext("local", app_name)
    print("MAIN_START")
    rdd_relation.multiple_country_mean_city_density_01(sc)
    print("MAIN_STOP")


if __name__ == "__main__":
    main()

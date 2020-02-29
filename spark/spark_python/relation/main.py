from pyspark import SparkContext

import rdd_relation
import dataframe_relation


def main(app_name="first app"):
    print("SPARK_START")
    sc = SparkContext("local", app_name)
    print("MAIN_START")
    dataframe_relation.capitol_to_country_density_ratio(sc)
    print("MAIN_STOP")


if __name__ == "__main__":
    main()

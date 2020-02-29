from typing import Optional, Dict, Any, List
from pyspark import SparkContext, RDD
import class_utils


def show_separately(rdd: RDD):
    rdd.foreach(lambda x: print(x))


def show_all(rdd: RDD):
    print(type(rdd), rdd.collect())


def convert_01(sc: SparkContext):
    cities = class_utils.read_cities()
    rdd = sc.parallelize(cities)

    def add_density(x):
        x['density'] = round(x['population'] / x['area'], 2)
        return x
    rdd = rdd.map(add_density)
    show_all(rdd)
    show_separately(rdd)


def convert_02(sc: SparkContext):
    cities = class_utils.read_cities()
    cities = [class_utils.City.from_dict(x) for x in cities]
    rdd = sc.parallelize(cities)

    def add_density(x: class_utils.City):
        return class_utils.CityWithDensity(
            name=x.name,
            country=x.country,
            population=x.population,
            area=x.area,
            density=round(x.population / x.area, 2)
        )
    rdd = rdd.map(add_density)  # no prompt in lambda
    show_all(rdd)
    show_separately(rdd)


def single_country_mean_city_density(sc: SparkContext, country_name="Poland"):
    countries = sc.parallelize(class_utils.read_countries())
    cities = sc.parallelize(class_utils.read_cities())

    country = countries.filter(lambda x: x['name'] == country_name).collect()
    iso_code = country[0]['iso']

    cities_density = cities\
        .filter(lambda x: x['country'] == iso_code)\
        .map(lambda x: {'name': x['name'], 'density': round(x['population'] / x['area'], 2)})
    show_all(cities_density)
    aggregation = cities_density.aggregate({'sum': 0, 'count': 0},
                                           lambda agg, x: {
                                               'sum': agg['sum'] + x['density'],
                                               'count': agg['count'] + 1
                                           },
                                           lambda agg1, agg2: {
                                               'sum': agg1['sum'] + agg2['sum'],
                                               'count': agg1['count'] + agg2['count']
                                           })
    print(aggregation['sum'])
    mean_density = round(aggregation['sum'] / aggregation['count'], 2)
    print(mean_density)


def multiple_country_mean_city_density_01(sc: SparkContext):
    countries = sc.parallelize(class_utils.read_countries())
    cities = sc.parallelize(class_utils.read_cities())

    iso_translator = countries.map(lambda x: (x['iso'], x['name'])).collectAsMap()

    result = cities \
        .filter(lambda x: x['country'] in iso_translator.keys()) \
        .map(lambda x: (iso_translator.get(x['country']),
                        {'name': x['name'], 'density': round(x['population'] / x['area'], 2)})) \
        .aggregateByKey({'sum': 0, 'count': 0},
                        lambda agg, x: {
                            'sum': agg['sum'] + x['density'],
                            'count': agg['count'] + 1
                        },
                        lambda agg1, agg2: {
                            'sum': agg1['sum'] + agg2['sum'],
                            'count': agg1['count'] + agg2['count']
                        })\
        .map(lambda x: {'country': x[0],
                        'mean_density': round(x[1]['sum'] / x[1]['count'], 2)})

    show_separately(result)


def multiple_country_mean_city_density_02(sc: SparkContext):
    countries = sc.parallelize(class_utils.read_countries())
    cities = sc.parallelize(class_utils.read_cities())

    iso_translator = countries.map(lambda x: (x['iso'], x['name'])).collectAsMap()

    def calculate_mean_density(cities_list: List[Dict[str, Any]]) -> float:
        return round(sum([c['density'] for c in cities_list]) / len(cities_list), 2)

    result = cities \
        .filter(lambda x: x['country'] in iso_translator.keys()) \
        .map(lambda x: {'name': x['name'],
                        'density': round(x['population'] / x['area'], 2),
                        'country_name': iso_translator.get(x['country'])}) \
        .groupBy(lambda x: x['country_name'])\
        .map(lambda x: {'country': x[0],
                        'mean_density': calculate_mean_density(x[1])})

    show_separately(result)


def capitol_to_country_density_ratio(sc: SparkContext):
    countries = sc.parallelize(class_utils.read_countries())
    cities = sc.parallelize(class_utils.read_cities())

    def with_density(x: Dict[str, Any]) -> Dict[str, Any]:
        return {**x, 'density': round(x['population'] / x['area'], 2)}

    # you cannot use one RDD inside function of other RDD
    result = countries\
        .map(lambda x: (x['capitol'], x))\
        .join(cities.map(lambda x: (x['name'], x)))\
        .map(lambda x: {'country': with_density(x[1][0]), 'capitol': with_density(x[1][1])})\
        .map(lambda x: {'country_name': x['country']['name'],
                        'capitol_name': x['capitol']['name'],
                        'country_density': x['country']['density'],
                        'capitol_density': x['capitol']['density'],
                        'density_ratio': round(x['capitol']['density'] / x['country']['density'], 2),
                        })

    show_separately(result)


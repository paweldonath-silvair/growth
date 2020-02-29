import json
from dataclasses import dataclass
from typing import List


@dataclass
class City:
    name: str
    country: str
    population: int
    area: float

    @staticmethod
    def from_dict(d) -> "City":
        return City(
            name=d['name'],
            country=d['country'],
            population=d['population'],
            area=d['area']
        )


@dataclass
class CityWithDensity(City):
    density: float


@dataclass
class CountryBase:
    name: str
    iso: str
    calling_code: str
    population: int
    area: float


@dataclass
class Country(CountryBase):
    capitol: str

    @staticmethod
    def from_dict(d) -> "Country":
        return Country(
            name=d['name'],
            iso=d['iso'],
            calling_code=d['calling_code'],
            population=d['population'],
            area=d['area'],
            capitol=d['capitol']
        )


@dataclass
class CountryNested(CountryBase):
    capitol: City

    @staticmethod
    def from_dict(d) -> "CountryNested":
        return CountryNested(
            name=d['name'],
            iso=d['iso'],
            calling_code=d['calling_code'],
            population=d['population'],
            area=d['area'],
            capitol=City.from_dict(d['capitol'])
        )


@dataclass
class CountryFull(CountryNested):
    cities: List[City]

    @staticmethod
    def from_dict(d) -> "CountryFull":
        return CountryFull(
            name=d['name'],
            iso=d['iso'],
            calling_code=d['calling_code'],
            population=d['population'],
            area=d['area'],
            capitol=City.from_dict(d['capitol']),
            cities=[City.from_dict(c) for c in d['cities']]
        )


def read_json(filename):
    with open(filename) as f:
        data = json.load(f)
    return data


def read_cities():
    return read_json('data/cities.json')


def read_countries():
    return read_json('data/countries.json')


def read_countries_nested():
    return read_json('data/countries_nested.json')


def read_countries_full():
    return read_json('data/countries_full.json')

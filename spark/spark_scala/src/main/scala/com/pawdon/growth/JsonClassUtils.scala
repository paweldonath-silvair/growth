package com.pawdon.growth

import io.circe.generic.auto._
import io.circe.generic.extras.Configuration
import io.circe.parser._

import scala.io.Source
import scala.util.{Failure, Success, Try}

case class City(name: String, country: String, population: Int, area: Double)

case class CityDensity(countryName: String, density: Double)

case class Country(name: String, iso: String, calling_code: String, population: Int, area: Double, capitol: String)

case class CountryFull(name: String, iso: String, calling_code: String, population: Int, area: Double,
                       capitol: Option[City], cities: Seq[City])

object JsonClassUtils {
  def readCities: Try[Seq[City]] = {
    val filename = "data/cities.json"
    val file = Source.fromFile(filename)
    val data: String = file.getLines().mkString
    file.close()
    decode[Seq[City]](data) match {
      case Left(error) => Failure(error)
      case Right(jsons) => Success(jsons)
    }
  }

  def readCountries: Try[Seq[Country]] = {
    val filename = "data/countries.json"
    val file = Source.fromFile(filename)
    val data: String = file.getLines().mkString
    file.close()
    decode[Seq[Country]](data) match {
      case Left(error) => Failure(error)
      case Right(jsons) => Success(jsons)
    }
  }

  def readCountriesFull: Try[Seq[CountryFull]] = {
    val filename = "data/countries_full.json"
    val file = Source.fromFile(filename)
    val data: String = file.getLines().mkString
    file.close()
    decode[Seq[CountryFull]](data) match {
      case Left(error) => Failure(error)
      case Right(jsons) => Success(jsons)
    }
  }
}

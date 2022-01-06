package com.vladislav

import org.apache.log4j.{Level, Logger}
import org.apache.spark._

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

case class Station(stationId: Int, name: String, lat: Double, long: Double, dockcount: Int,
                   landmark: String, installation: String)

case class Trip(tripId: Int, duration: Int, startDate: LocalDateTime, startStation: String,
                startStationId: Int, endDate: LocalDateTime, endStation: String, endStationId: Int,
                bikeId: Int, subscriptionType: String, zipCode: String)

object Main {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(cfg)

    val tripData = sc.textFile("file:///C:/Users/Vladislav/Desktop/Big data/L1/trip.csv")
    // запомним заголовок, чтобы затем его исключить
    val tripsHeader = tripData.first
    val trips = tripData.filter(_ != tripsHeader).map(_.split(",", -1))
    val tripsInternal = trips.mapPartitions(rows => {
      val timeFormat = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
      rows.map(row => Trip(tripId = row(0).toInt,
        duration = row(1).toInt,
        startDate = LocalDateTime.parse(row(2), timeFormat),
        startStation = row(3),
        startStationId = row(4).toInt,
        endDate = LocalDateTime.parse(row(5), timeFormat),
        endStation = row(6),
        endStationId = row(7).toInt,
        bikeId = row(8).toInt,
        subscriptionType = row(9),
        zipCode = row(10))
      )
    })

    val stationData = sc.textFile("file:///C:/Users/Vladislav/Desktop/Big data/L1/station.csv")
    val stationsHeader = stationData.first
    val stations = stationData.filter(_ != stationsHeader).map(_.split(",", -1))
    val stationsInternal = stations.map(row =>
      Station(
        stationId = row(0).toInt,
        name = row(1),
        lat = row(2).toDouble,
        long = row(3).toDouble,
        dockcount = row(4).toInt,
        landmark = row(5),
        installation = row(6)
      )
    )

    val bikeWithMaxRun = tripsInternal.map(x => (x.bikeId, x.duration)).reduceByKey(_ + _)
      .max()(Ordering.by[(Int, Int), Int](_._2))
    println("Bike with max run id: " + bikeWithMaxRun._1 + ", run: " + bikeWithMaxRun._2)

    val distancesBetweenStations = sc.broadcast(stationsInternal.cartesian(stationsInternal).map(sp =>
      ((sp._1.stationId, sp._2.stationId), distanceBetween(sp._1, sp._2))
    ).collect().toMap)

    val bikes = tripsInternal.map(trip => trip.bikeId).distinct().count()
    println("Bikes count: " + bikes)

    val bikesWithMoreThan3Hours = tripsInternal.keyBy(rec => rec.bikeId)
      .mapValues(x => x.duration)
      .reduceByKey(_ + _)
      .filter(p => p._2 > 10800)
    println("Bikes with duration more than 3 hours: " + bikesWithMoreThan3Hours.count())
    //bikesWithMoreThan3Hours.foreach(println)

    val biggestDistanceBetweenStations = distancesBetweenStations.value.max(Ordering.by[((Int, Int), Double), Double](_._2))
    println("Biggest distance between stations " + biggestDistanceBetweenStations._1._1
      + " and " + biggestDistanceBetweenStations._1._2 + ": " + biggestDistanceBetweenStations._2)

    val bikeWithMaxRunPath = tripsInternal.filter(t => t.bikeId == bikeWithMaxRun._1)
      .sortBy(_.startDate.toInstant(ZoneOffset.UTC).toEpochMilli)
    println("Bike with max run path:")
    bikeWithMaxRunPath.foreach(println)

    sc.stop()
  }

  def distanceBetween(a: Station, b: Station): Double =
    Math.sqrt(Math.pow(b.lat - a.lat, 2) + Math.pow(b.long - a.long, 2))
}

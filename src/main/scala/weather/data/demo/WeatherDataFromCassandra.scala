package weather.data.demo

import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import com.github.nscala_time.time.Imports._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import weather.data.demo.WeatherDataSchema.{WEATHER_DATA_TABLE, WEATHER_STATION_TABLE, KEYSPACE}

import scala.collection.{SortedMap, mutable, Map}

object WeatherDataFromCassandra {


  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("weather_data_from_cassandra")
      .setMaster("local[2]")
      .set("spark.cassandra.input.page.row.size","10000")
      .set("spark.cassandra.input.split.size","1000000")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    // Fetch only FRENCH weather stations
    val frenchWeatherStations = sc.cassandraTable[WeatherStation](KEYSPACE, WEATHER_STATION_TABLE)
        .filter(_.countryCode == "FR").map(station => (station.id, station.name)).collectAsMap()

    val frenchStationsMap: Broadcast[Map[String, String]] = sc.broadcast(frenchWeatherStations)

    val startTime = DateTime.now

    // Calculate average daily temperature & pressure over all French stations, between March 2014 and June 2014
    val frenchSamples: Map[String, Sample] = sc.cassandraTable[MonthlySampleData](KEYSPACE, WEATHER_DATA_TABLE)
      .select("weather_station", "year", "month", "temperature", "pressure")
      .filter(instance => frenchStationsMap.value.contains(instance.weatherStation)) //only French stations
      .filter(instance => instance.month >= 3 && instance.month <= 6) // between March 2014 and June 2014
      .map(data => {
        val date = new mutable.StringBuilder().append(data.year).append("-").append(data.month.formatted("%02d")).toString()
        (date, Sample(data.temperature, data.pressure))
      })
      .mapValues { case sample => (sample, 1)} // add counter. (date, sample) becomes (date, (sample,1))
      .reduceByKey { case ((sample1, count1), (sample2, count2)) => (sample1 + sample2, count1 + count2)} // sum temperature,pressure,occurence
      .mapValues { case (sample, totalCount) => sample.avg(totalCount)} // average on temperature & pressure
      .collectAsMap()



    println("")
    println("/************************************ RESULT *******************************/")
    println("")
    println((SortedMap.empty[String, Sample] ++ frenchSamples))
    println("")
    println("/***************************************************************************/")
    val endTime = DateTime.now

    println(s"Job start time : $startTime, end time : $endTime")

  }

  case class Sample(temperature:Float,pressure:Float) {

    def +(that: Sample) = {
      Sample(this.temperature+that.temperature,this.pressure+that.pressure)
    }

    def avg(totalCount:Int) = {
      Sample(this.temperature/totalCount,this.pressure/totalCount)
    }

    override def toString: String = s"""(temperature = $temperature °C, pressure = $pressure hPa)"""
  }
  
  case class WeatherStation(id:String, name:String, countryCode:String, stateCode:String, callSign:String,lat:Float,long:Float,elevation:Float)

  case class MonthlySampleData(weatherStation:String, year:Int, month:Int, temperature:Float, pressure:Float)

}

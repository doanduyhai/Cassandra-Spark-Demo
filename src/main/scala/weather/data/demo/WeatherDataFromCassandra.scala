package weather.data.demo

import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object WeatherDataFromCassandra {


  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("write_csv_to_cassandra")
      .setMaster("local[4]")
      .set("spark.cassandra.input.page.row.size","10000")
      .set("spark.cassandra.input.split.size","1000000")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    // Fetch only FRENCH weather stations
    val frenchWeatherStations = sc.cassandraTable[WeatherStation](WeatherDataSchema.KEYSPACE, WeatherDataSchema.WEATHER_STATION_TABLE)
        .filter(_.countryCode == "FR").map(station => (station.id, station.name)).collectAsMap()

    // Calculate average daily temperature & pressure over all French stations, between March 2014 and June 2014
    val frenchSamples = sc.cassandraTable[MonthlySampleData](WeatherDataSchema.KEYSPACE, WeatherDataSchema.WEATHER_DATA_TABLE)
      .select("weather_station","year","month","temperature","pressure")
      .filter(instance => frenchWeatherStations.contains(instance.weatherStation)) //only French stations
      .filter(instance => instance.month >= 3 && instance.month <= 6) // between March 2014 and June 2014
      .map(data => {
        val date = new StringBuilder().append(data.year).append("-").append(data.month.formatted("%02d")).toString()
        (date, Sample(data.temperature, data.pressure))
      })
      .mapValues{case sample => (sample,1)} // add counter. (date, sample) becomes (date, (sample,1))
      .reduceByKey{ case ((sample1,count1),(sample2,count2)) => (sample1+sample2,count1+count2)} // sum temperature,pressure,occurence
      .mapValues{case (sample,totalCount) => sample.avg(totalCount)} // average on temperature & pressure
      .sortByKey()
      .collectAsMap()

    println("")
    println("/************************************ RESULT *******************************/")
    println("")
    println(frenchSamples)
    println("")
    println("/***************************************************************************/")
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

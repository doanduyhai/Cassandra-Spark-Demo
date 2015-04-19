package weather.data.demo

import com.datastax.spark.connector.{SomeColumns, _}
import com.github.nscala_time.time.Imports._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Period

object WeatherDataIntoCassandra {

  val WEATHER_2014_CSV: String = "/Users/archinnovinfo/perso/spark_data/Weather Data 2014.csv"
  val TABLE_COLUMNS = SomeColumns("weather_station", "year", "month", "day", "hour",
                          "temperature", "dewpoint", "pressure", "wind_direction", "wind_speed",
                          "sky_condition", "sky_condition_text", "one_hour_precip", "six_hour_precip")


  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("weather_data_into_cassandra")
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.output.batch.size.bytes", "1024")
      .set("spark.cassandra.output.batch.size.rows", "100")

    val sc = new SparkContext(conf)

    val skyConditions =  sc.cassandraTable[SkyCondition](WeatherDataSchema.KEYSPACE, WeatherDataSchema.SKY_CONDITION_TABLE).collect()
                        .map(instance => (instance.code, instance.condition)).toMap;

    val skyConditionBc = sc.broadcast(skyConditions)

    val startTime = DateTime.now

    // Example of raw data
    // 010010:99999,
    // 2014,01,01,00,
    // -0.8,-4.0,1012.2,290.0,6.0,
    // 1.0, lookupForSky Condition, 0.0, 0.0
    sc.textFile(WEATHER_2014_CSV)
      .map{line => {
        val lines = line.split(",")
        val skyConditionCode = lines(10).toFloat.toInt

      (lines(0), lines(1).toInt, lines(2).toInt, lines(3).toInt, lines(4).toInt,
        lines(5).toFloat, lines(6).toFloat, lines(7).toFloat, lines(8).toFloat.toInt, lines(9).toFloat,
        skyConditionCode, skyConditionBc.value.get(skyConditionCode), lines(11).toFloat, lines(12).toFloat)
      }}
      .saveToCassandra(WeatherDataSchema.KEYSPACE, WeatherDataSchema.WEATHER_DATA_TABLE, TABLE_COLUMNS)

    val endTime = DateTime.now

    val period: Period = new Period(endTime, startTime)


    println(s"Job duration (sec) : ${period.getSeconds}")
  }


  case class SkyCondition(code:Int, condition:String)

}

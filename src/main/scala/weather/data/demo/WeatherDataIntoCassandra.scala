package weather.data.demo

import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.spark.{SparkConf, SparkContext}

object WeatherDataIntoCassandra {

  val WEATHER_2014_CSV: String = "/path/to/2014.csv"
  val TABLE_COLUMNS = Seq("weather_station", "year", "month", "day", "hour",
                          "temperature", "dewpoint", "pressure", "wind_direction", "wind_speed",
                          "sky_condition", "sky_condition_text", "one_hour_precip", "six_hour_precip")


  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("write_csv_to_cassandra")
      .setMaster("local[4]")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    val skyConditions =  sc.cassandraTable[SkyCondition](WeatherDataSchema.KEYSPACE, WeatherDataSchema.SKY_CONDITION_TABLE).collect()
                        .map(instance => (instance.code, instance.condition)).toMap;


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
        skyConditionCode, skyConditions.get(skyConditionCode), lines(11).toFloat, lines(12).toFloat)
      }}
      .saveToCassandra(WeatherDataSchema.KEYSPACE, WeatherDataSchema.WEATHER_DATA_TABLE, SomeColumns(TABLE_COLUMNS:_*))
  }

  case class SkyCondition(code:Int, condition:String)

}

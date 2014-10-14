package weather.data.demo

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

object WeatherDataSchema {

   val KEYSPACE = "spark_demo"
   val WEATHER_STATION_TABLE = "weather_station"
   val WEATHER_DATA_TABLE = "raw_weather_data"
   val SKY_CONDITION_TABLE = "sky_condition_lookup"

   def prepareSchemaAndCleanData(conf: SparkConf) {
     CassandraConnector(conf).withSessionDo { session =>

       session.execute(s"TRUNCATE $KEYSPACE.$WEATHER_DATA_TABLE")
     }
   }
 }

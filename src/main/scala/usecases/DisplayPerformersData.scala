package usecases

import analytics.music.Schema._
import com.datastax.spark.connector._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

object DisplayPerformersData {

   /**
    *
    *   CREATE TABLE IF NOT EXISTS performers (
    *     name TEXT,
    *     country TEXT,
    *     gender TEXT,
    *     type TEXT,
    *     born TEXT,
    *     died TEXT,
    *     styles LIST<TEXT>,
    *     PRIMARY KEY (name)
    *   );
    */

   case class Performer(name: String, country: String, gender:String, `type`:String,born:String,died:String, styles:Set[String])

   def main (args: Array[String]) {

     val conf = new SparkConf(true)
       .setAppName("data_display")
       .setMaster("local")
       .set("spark.cassandra.connection.host", "localhost")

     val sc = new SparkContext(conf)



     sc.cassandraTable[Performer](KEYSPACE, PERFORMERS)
     .filter(bean => StringUtils.isNotBlank(bean.died))
     .map(bean => bean.died)
     .collect()
     .sorted
     .toSet
     .foreach[Unit](x => println(x))

     sc.stop()
   }


 }

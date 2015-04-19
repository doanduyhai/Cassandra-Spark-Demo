package usecases

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import analytics.music.Schema._

import scala.util.matching.Regex

object DataCleaningForPerformers {

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
  val YearPattern = "^[0-9]{4}$".r
  val YearMonthPattern = "^[0-9]{4}-[0-9]{2}$".r
  case class Performer(name: String, country: String, gender:String, `type`:String,born:String,died:String, styles:Set[String])

  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("data_cleaning")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    println("\n ------------ Cleaning data ----------------- \n")

    val cache: CassandraTableScanRDD[Performer] = sc
      .cassandraTable[Performer](KEYSPACE, PERFORMERS).cache()

    //Fix blank country
    cache
      .filter(bean => StringUtils.isBlank(bean.country))
      .map(bean => (bean.name,"Unknown"))
      /**
       * Equivalent to INSERT INTO performers(name,country) VALUES(xxx,yyy)
       * equivalent to UPDATE performers SET country = yyy WHERE name=xxx
       */
      .saveToCassandra(KEYSPACE, PERFORMERS,SomeColumns("name","country"))
    
    //Normalize born date
    cache
      .filter(bean => matchPattern(bean.born,YearPattern))
      .map(bean => (bean.name,bean.born+"-01-01"))
      .saveToCassandra(KEYSPACE, PERFORMERS,SomeColumns("name","born"))

    cache
      .filter(bean => matchPattern(bean.born,YearMonthPattern))
      .map(bean => (bean.name,bean.born+"-01"))
      .saveToCassandra(KEYSPACE, PERFORMERS,SomeColumns("name","born"))

    //Normalize died date
    cache
      .filter(bean => matchPattern(bean.died,YearPattern))
      .map(bean => (bean.name,bean.died+"-01-01"))
      .saveToCassandra(KEYSPACE, PERFORMERS,SomeColumns("name","died"))

    cache
      .filter(bean => matchPattern(bean.died,YearMonthPattern))
      .map(bean => (bean.name,bean.died+"-01"))
      .saveToCassandra(KEYSPACE, PERFORMERS,SomeColumns("name","died"))

    sc.stop()
  }

  private def matchPattern(input:String, pattern: Regex):Boolean = {
    if(StringUtils.isBlank(input)) return false

    pattern findFirstIn input match {
      case Some(c) => true
      case None => false
    }
  }

}

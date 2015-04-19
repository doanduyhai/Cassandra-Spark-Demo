package usecases

import analytics.music.Schema._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}

object MigrateAlbumsData {

  /**
   *
   *   CREATE TABLE spark_demo.albums (
   *       title text PRIMARY KEY,
   *       country text,
   *       performer text,
   *       quality text,
   *       status text,
   *       year int
   *   )
   *
   *   CREATE TABLE spark_demo.albums_by_country (
   *       title text,
   *       country text,
   *       performer text,
   *       quality text,
   *       status text,
   *       year int,
   *       PRIMARY KEY((country,year),title)
   *   )
   */

  case class Albums(title: String, country: String, performer:String, quality:String,status:String,year:Int)

   def main (args: Array[String]) {

     val conf = new SparkConf(true)
       .setAppName("data_migration")
       .setMaster("local")
       .set("spark.cassandra.connection.host", "localhost")

     val sc = new SparkContext(conf)

     CassandraConnector(conf).withSessionDo {
       session => session.execute(
         """
           CREATE TABLE IF NOT EXISTS spark_demo.albums_by_country (
              title text,
              country text,
              performer text,
              quality text,
              status text,
              year int,
              PRIMARY KEY((country,year),title)
           )
         """.stripMargin)
     }

     sc.cassandraTable[Albums](KEYSPACE, ALBUMS)
     .saveToCassandra(KEYSPACE,ALBUMS_BY_COUNTRY)
     sc.stop()
   }


 }

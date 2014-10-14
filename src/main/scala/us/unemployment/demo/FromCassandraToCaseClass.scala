package us.unemployment.demo

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object FromCassandraToCaseClass {

  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("read_csv_from_cassandra_into_case_class")
      .setMaster("local[4]")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    sc.cassandraTable[UsUnemployment](UsUnemploymentSchema.KEYSPACE, UsUnemploymentSchema.TABLE)
    .foreach(println(_))
  }


}

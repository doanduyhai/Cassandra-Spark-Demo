package us.unemployment.demo

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import us.unemployment.demo.UsUnemploymentSchema.{TABLE, KEYSPACE}

object FromCassandraToCaseClass {

  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("read_csv_from_cassandra_into_case_class")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    println("\n ------------ Results ----------------- \n")

    sc.cassandraTable[UsUnemployment](KEYSPACE, TABLE)
    .sortBy(record => record.year)
    .foreach(println(_))
    sc.stop()
  }


}

package us.unemployment.demo

import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.{SparkConf, SparkContext}
import us.unemployment.demo.UsUnemploymentSchema.{TABLE, KEYSPACE}

object FromCassandraToRow {

  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("read_csv_from_cassandra_into_case_class")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")


    val sc = new SparkContext(conf)

    println("\n ------------Cassandra Row ----------------- \n")

    sc.cassandraTable(KEYSPACE, TABLE)
    .foreach( row => {
        val year = row.getInt("year")
        val unemployedPercentageToLabor = row.getDouble("unemployed_percentage_to_labor")
        println(s"""Year($year), unemployment % : $unemployedPercentageToLabor""")
      }
    )

    // or
    val unemployment: CassandraRDD[(Int, Double)] = sc.cassandraTable(KEYSPACE, TABLE)
      .select("year", "unemployed_percentage_to_labor").as((_: Int, _: Double))

    println("\n ------------Tuples ----------------- \n")

    unemployment
      .sortByKey()
      .collect().foreach{case (year,percentage) => println(s""" Year($year), unemployment % : $percentage""")}

    sc.stop()
  }
}

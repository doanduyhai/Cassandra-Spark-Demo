package us.unemployment.demo

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object FromCassandraToRow {

  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("read_csv_from_cassandra_into_case_class")
      .setMaster("local[4]")
      .set("spark.cassandra.connection.host", "localhost")


    val sc = new SparkContext(conf)

    sc.cassandraTable(UsUnemploymentSchema.KEYSPACE, UsUnemploymentSchema.TABLE)
    .foreach( row => {
        val year = row.getInt("year")
        val unemployedPercentageToLabor = row.getDouble("unemployed_percentage_to_labor")
        println(s"""Year($year), unemployment % : $unemployedPercentageToLabor""")
      }
    )


  }
}

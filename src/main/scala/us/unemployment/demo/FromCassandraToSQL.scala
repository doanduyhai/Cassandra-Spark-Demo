package us.unemployment.demo

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import us.unemployment.demo.UsUnemploymentSchema.{KEYSPACE, TABLE}

object FromCassandraToSQL {

  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("read_csv_from_cassandra_into_case_class")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    val cc = new CassandraSQLContext(sc)

    cc.setKeyspace(KEYSPACE)

    val row: SchemaRDD = cc.cassandraSql(s"SELECT year,unemployed_percentage_to_labor " +
                                         s"FROM $TABLE WHERE unemployed_percentage_to_labor > 8 " +
                                         s"ORDER BY year DESC")

    row.collect().foreach(row => println(s" Year(${row(0)}): ${row(1)}%"))

    sc.stop()
  }


}

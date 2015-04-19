package usecases

import analytics.music.Schema._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector}
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object CrossClusterDataMigration {

  case class Performer(name: String, country: String, gender:String, `type`:String,born:String,died:String, styles:Set[String])

  // DO NOT EXECUTE THIS CLASS, IT IS ONLY A CODE SAMPLE
  @deprecated
   def main (args: Array[String]) {

     val confCluster1 = new SparkConf(true)
       .setAppName("cross_cluster_data_migration")
       .setMaster("master_ip")
       .set("spark.cassandra.connection.host", "cluster_1_hostnames")

     val confCluster2 = new SparkConf(true)
       .setAppName("data_migration")
       .setMaster("master_ip")
       .set("spark.cassandra.connection.host", "cluster_2_hostnames")


     val sc = new SparkContext(confCluster1)

     sc.cassandraTable[Performer](KEYSPACE,PERFORMERS)
     .map[Performer](???)
     .saveToCassandra(KEYSPACE,PERFORMERS)
     (CassandraConnector(confCluster2),implicitly[RowWriterFactory[Performer]])

   }

 }

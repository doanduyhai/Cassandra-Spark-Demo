package usecases

import analytics.music.Schema._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.{SparkConf, SparkContext}

object CrossDCDataMigration {

  case class Performer(name: String, country: String, gender:String, `type`:String,born:String,died:String, styles:Set[String])

  // DO NOT EXECUTE THIS CLASS, IT IS ONLY A CODE SAMPLE
  @deprecated
   def main (args: Array[String]) {

     val confDC1 = new SparkConf(true)
       .setAppName("cross_DC_data_migration")
       .setMaster("master_ip")
       .set("spark.cassandra.connection.host", "DC_1_hostnames")
       .set("spark.cassandra.connection.local_dc", "DC_1")

     val confDC2 = new SparkConf(true)
       .setAppName("data_migration")
       .setMaster("master_ip")
       .set("spark.cassandra.connection.host", "DC_2_hostnames")
       .set("spark.cassandra.connection.local_dc", "DC_2")


     val sc = new SparkContext(confDC1)

     sc.cassandraTable[Performer](KEYSPACE,PERFORMERS)
     .map[Performer](???)
     .saveToCassandra(KEYSPACE,PERFORMERS)
     (CassandraConnector(confDC2),implicitly[RowWriterFactory[Performer]])

   }

 }

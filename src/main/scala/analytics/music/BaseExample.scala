package analytics.music

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import analytics.music.Schema._
import analytics.music.Constants._

trait BaseExample {

  def buildSparkContext(exerciseName: String):SparkContext = {

    val conf = new SparkConf(true)
      .setAppName(exerciseName)
      .setMaster(LOCAL_MODE)
      .set(CASSANDRA_HOST_NAME_PARAM, CASSANDRA_IP)


    if (Constants.TABLES.contains(exerciseName)) {
      CassandraConnector(conf).withSessionDo {
        session => session.execute(s"TRUNCATE ${KEYSPACE}.${Constants.TABLES(exerciseName)}")
      }
    }

    new SparkContext(conf)
  }
}

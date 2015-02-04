package twitter.stream

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

object StreamingSchema {

  val KEYSPACE = "spark_demo"
  val TABLE = "twitter_stream"

  def prepareSchemaAndCleanData(conf: SparkConf) {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute( s"""CREATE TABLE IF NOT EXISTS $KEYSPACE.$TABLE (
                            topic text,
                            interval text,
                            mentions counter,
                            PRIMARY KEY(topic, interval)
                          ) WITH CLUSTERING ORDER BY (interval DESC)
      """)
      session.execute(s"TRUNCATE $KEYSPACE.$TABLE")
    }
  }
}

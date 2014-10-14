package us.unemployment.demo

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

object UsUnemploymentSchema {

  val KEYSPACE = "spark_demo"
  val TABLE = "us_unemployment_stats"

  def prepareSchemaAndCleanData(conf: SparkConf) {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute( s"""CREATE TABLE IF NOT EXISTS $KEYSPACE.$TABLE (
                            year int PRIMARY KEY,
                            civil_non_institutional_count int,
                            civil_labor_count int,
                            labor_population_percentage double,
                            employed_count int,
                            employed_percentage double,
                            agriculture_part_count int,
                            non_agriculture_part_count int,
                            unemployed_count int,
                            unemployed_percentage_to_labor double,
                            not_labor_count int,
                            footnotes text)
      """)
      session.execute(s"TRUNCATE $KEYSPACE.$TABLE")
    }
  }
}

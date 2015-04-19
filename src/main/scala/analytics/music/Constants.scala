package analytics.music

import analytics.music.Schema._

object Constants {

  val CASSANDRA_HOST_NAME_PARAM = "spark.cassandra.connection.host"
  val CASSANDRA_IP = "localhost"
  val LOCAL_MODE = "local"
  
  val EXAMPLE_1 = "example1"
  val EXAMPLE_2 = "example2"
  val EXAMPLE_3 = "example3"
  val EXAMPLE_4 = "example4"
  val EXAMPLE_5 = "example5"
  val INTERACTIVE_ANALYTICS_WITH_DSE = "interactive_analytics_with_DSE"

  val TABLES = Map( EXAMPLE_1 -> PERFORMERS_BY_STYLE,
                    EXAMPLE_2 -> PERFORMERS_DISTRIBUTION_BY_STYLE,
                    EXAMPLE_3 -> TOP_10_STYLES,
                    EXAMPLE_4 -> ALBUMS_BY_DECADE_AND_COUNTRY,
                    EXAMPLE_5 -> ALBUMS_BY_DECADE_AND_COUNTRY_SQL)
}

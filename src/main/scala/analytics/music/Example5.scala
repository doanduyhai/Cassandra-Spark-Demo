package analytics.music

import com.datastax.spark.connector._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import analytics.music.Constants._
import analytics.music.Schema._

object Example5 extends BaseExample {

  def main (args: Array[String]) {

    val sc = buildSparkContext(EXAMPLE_5)

    // Create an new Cassandra SQL context
    val cc = new CassandraSQLContext(sc)

    // Set the Cassandra keyspace to be used
    cc.setKeyspace(KEYSPACE)

    // Register computeDecade() as a SparkSQL function
    cc.registerFunction("computeDecade", computeDecade _)

    /*
     * CREATE TABLE IF NOT EXISTS performers (
     *   name TEXT,
     *   ...
     *   country TEXT,
     *   ...
     * );
     *
     *
     * CREATE TABLE IF NOT EXISTS albums (
     *   ...
     *   title TEXT,
     *   year INT,
     *   performer TEXT,
     *   ...
     * );
     *
     *  - SELECT computeDecade(album release year),artist country, count(album title)
     *  - FROM performers & albums JOINING on performers.name=albums.performer
     *  - WHERE performer' country is not null and different than 'Unknown'
     *  - AND album' release year is greater or equal to 1900
     *  - GROUP BY computeDecade(album release year) and artist country
     *  - HAVING count(albums title)>250 to filter out low values count countries
     */

    val query: String = """
        SELECT computeDecade(a.year),p.country,count(a.title)
        FROM performers p JOIN albums a
        ON p.name = a.performer
        WHERE p.country is not null
        AND p.country != 'Unknown'
        AND a.year >= 1900
        GROUP BY computeDecade(a.year),p.country
        HAVING count(a.title) > 250
      """.stripMargin

    // Execute the SQL statement against Cassandra and Spark
    val rows: SchemaRDD = cc.cassandraSql(query)

    // Map back the Schema RDD into a triplet (decade,country,count)
    rows.map(row => (row(0),row(1),row(2)))
      .saveToCassandra(KEYSPACE, ALBUMS_BY_DECADE_AND_COUNTRY_SQL, SomeColumns("decade","country","album_count"))

    sc.stop()
  }

  def computeDecade(year:Int):String = {
    s"${(year/10)*10}-${((year/10)+1)*10}"
  }
}

package analytics.music

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import analytics.music.Constants._
import analytics.music.Schema._

object Example4 extends BaseExample {

  case class PerformerAndYear(name: String, year:Int)
  def main (args: Array[String]) {

    val sc = buildSparkContext(EXAMPLE_4)


    /*
     * CREATE TABLE IF NOT EXISTS performers (
     *   name TEXT,
     *   ...
     *   country TEXT,
     *   ...,
     *   PRIMARY KEY(name)
     * );
     *
     *
     * CREATE TABLE IF NOT EXISTS albums (
     *   ...
     *   year INT,
     *   performer TEXT,
     *   ...,
     * );
     */


    val albums:RDD[(String,Int)] = sc.cassandraTable(KEYSPACE, ALBUMS)
      .select("performer","year")
      .as((_:String,_:Int))
      .filter{case(_,year) => year >= 1900}


    val join: RDD[(String, (String, Int))] = albums
//      .map(couple => PerformerAndYear(couple._1,couple._2))
      /**
       * For tuples, the mapping with PERFORMERS partition key is
       * done by comparing type. albums._1 is of type String as well
       * as performers.name
       *
       * If you want to control the mapping to performer' partition key explicitly,
       * you can use Scala case classe PerformerAndYear. In this case the mapping
       * is done using field name e.g. PerformerAndYear."name" will match
       * the table performer "name" column
       */
      .repartitionByCassandraReplica(KEYSPACE, PERFORMERS,1)
      .joinWithCassandraTable[(String,String)](KEYSPACE, PERFORMERS, SomeColumns("name","country"))
      .on(SomeColumns("name"))
      .filter{case(_,(_,country)) => country != null && country != "Unknown"}
      .map{case((performer,year),(name,country)) => (name,(country,year))}


    join
      //RDD into a ((decade,country),1) using the computeDecade() pre-defined function
      .map { case(performer,(country,year)) => ((computeDecade(year),country),1) }
      //Reduce by key to count the number of occurrence for each key (decade,country)
      .reduceByKey{case(left,right) => left+right}
      //Flatten the tuple to (decade,country,count) triplet
      .map { case((decade,country),count) => (decade,country,count)}
      .saveToCassandra(KEYSPACE, ALBUMS_BY_DECADE_AND_COUNTRY, SomeColumns("decade","country","album_count"))

    sc.stop()
  }

  def computeDecade(year:Int):String = {
    s"${(year/10)*10}-${((year/10)+1)*10}"
  }
}

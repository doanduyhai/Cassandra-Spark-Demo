package analytics.music

import analytics.music.Schema.KEYSPACE
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import analytics.music.Constants._
import analytics.music.Schema._
import org.apache.spark.sql.cassandra.CassandraSQLContext

object Example3 extends BaseExample {

  def main (args: Array[String]) {


    val sc = buildSparkContext(EXAMPLE_3)

    val cc = new CassandraSQLContext(sc)

    /*
     * Read data from 'performers_distribution_by_style' table
     */
    val groupRDD:RDD[(String,String,Int)] = cc.cassandraSql( s"""
      SELECT *
      FROM $KEYSPACE.$PERFORMERS_DISTRIBUTION_BY_STYLE
      WHERE style != 'Unknown' AND type = 'group'
      """)
      .map(row => (row.getString(0),row.getString(1),row.getInt(2)))
      .sortBy(tuple => tuple._3,false,1)

    groupRDD.cache()

    val soloArtistRDD:RDD[(String,String,Int)] = cc.cassandraSql( s"""
      SELECT *
      FROM $KEYSPACE.$PERFORMERS_DISTRIBUTION_BY_STYLE
      WHERE style != 'Unknown' AND type = 'artist'
      """)
      .map(row => (row.getString(0),row.getString(1),row.getInt(2)))
      .sortBy(tuple => tuple._3,false,1)

    soloArtistRDD.cache()

    // Count total number of artists having styles that are not in the top 10
    val otherStylesCountForGroup:Int = groupRDD
      .collect()  //Fetch the whole RDD back to driver program
      .drop(10) //Drop the first 10 top styles
      .map{case(_,_,count)=>count} //Extract the count
      .sum //Sum up the count

    // Count total number of groups having styles that are not in the top 10
    val otherStylesCountForArtist:Int = soloArtistRDD
      .collect() //Fetch the whole RDD back to driver program
      .drop(10) //Drop the first 10 top styles
      .map{case(_,_,count)=>count} //Extract the count
      .sum //Sum up the count


    // Take the top 10 styles for groups, with a count for all other styles
    val top10Groups = groupRDD.take(10) :+ ("group","Others",otherStylesCountForGroup)

    // Take the top 10 styles for artists, with a count for all other styles
    val top10Artists = soloArtistRDD.take(10) :+ ("artist","Others",otherStylesCountForArtist)

    /*
     * Remark: by calling take(n), all the data are shipped back to the driver program
     * the output of take(n) is no longer an RDD but a simple Scala collection
     */

    // Merge both list and save back to Cassandra
    sc.parallelize(top10Artists.toList ::: top10Groups.toList)
      .saveToCassandra(KEYSPACE,TOP_10_STYLES,SomeColumns("type","style","count"))

    sc.stop()
  }
}
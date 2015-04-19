package analytics.music

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.rdd.RDD
import analytics.music.Constants._
import analytics.music.Schema._

object Example3 extends BaseExample {

  def main (args: Array[String]) {


    val sc = buildSparkContext(EXAMPLE_3)

    /*
     * Read data from 'performers_distribution_by_style' table
     */
    val artists:CassandraRDD[(String,String,Int)] = sc
      .cassandraTable(KEYSPACE, PERFORMERS_DISTRIBUTION_BY_STYLE)
      .select("type","style","count")
      .as((_:String,_:String,_:Int))

    val sortedPerformers:RDD[(String,String,Int)] = artists
      .filter {case (_,style,_) => style != "Unknown"}
      .sortBy[Int](tuple => tuple._3,false,1)

    // Put the sorted RDD in cache for re-use
    sortedPerformers.cache()

    // Extract styles for groups
    val groupsStyles: RDD[(String, String, Int)] = sortedPerformers
      .filter {case(performer_type,_,_) => performer_type == "group"}

    // Extract styles for artists
    val artistStyles: RDD[(String, String, Int)] = sortedPerformers
      .filter {case(performer_type,_,_) => performer_type == "artist"}

    // Cache the groupStyles
    groupsStyles.cache()

    // Cache the artistStyles
    artistStyles.cache()

    // Count total number of artists having styles that are not in the top 10
    val otherStylesCountForGroup:Int = groupsStyles
      .collect()  //Fetch the whole RDD back to driver program
      .drop(10) //Drop the first 10 top styles
      .map{case(_,_,count)=>count} //Extract the count
      .sum //Sum up the count

    // Count total number of groups having styles that are not in the top 10
    val otherStylesCountForArtist:Int = artistStyles
      .collect() //Fetch the whole RDD back to driver program
      .drop(10) //Drop the first 10 top styles
      .map{case(_,_,count)=>count} //Extract the count
      .sum //Sum up the count


    // Take the top 10 styles for groups, with a count for all other styles
    val top10Groups = groupsStyles.take(10) :+ ("group","Others",otherStylesCountForGroup)

    // Take the top 10 styles for artists, with a count for all other styles
    val top10Artists = artistStyles.take(10) :+ ("artist","Others",otherStylesCountForArtist)

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

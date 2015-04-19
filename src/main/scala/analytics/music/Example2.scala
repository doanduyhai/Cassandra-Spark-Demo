package analytics.music

import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import analytics.music.Constants._
import analytics.music.Schema._

object Example2 extends BaseExample {

  def main (args: Array[String]) {

    val sc = buildSparkContext(EXAMPLE_2)

    /*
     * Read columns "type" and "styles" from table 'performers'
     * and save them as (String,List[String]) RDDs
     * using the .as((_:String,_:List[String])) type conversion function
     * Normalize the performer type by filtering out 'Unknown' types
     */
    val rdd:RDD[(String,List[String])] = sc.cassandraTable(KEYSPACE, PERFORMERS)
      .select("type","styles")
      .as((_:String,_:List[String]))
      .map{case(performer_type,style) => (PERFORMERS_TYPES.getOrElse(performer_type,"Unknown"),style)}
      .filter{case(performer_type,_) => performer_type != "Unknown"}


    /*
    * Transform the previous tuple RDDs into a key/value RDD (PairRDD) of type
    * ((String,String),Integer). The (String,String) pair is the key(performer type,style)
    * The Integer value should be set to 1 for each element of the RDD
    */
    val pairs:RDD[((String,String),Int)] = rdd.flatMap{
      case(performer_type,styles)=>styles.map(style => ((performer_type,style),1))
    }

    /*
     * Reduce the previous tuple of ((performer type,style),1) by
     * adding up all the 1's into a  ((performer type,style),count)
     */
    val reduced: RDD[((String, String), Int)] = pairs.reduceByKey{ case(left,right) => left+right}

    /*
     * Flatten the ((performer type,style),count) into
     *  (performer type,style,count)
     */
    val aggregated:RDD[(String,String,Int)] = reduced.map{
      case((performer_type,style),count) => (performer_type,style,count)
    }


    //Save data back to the performers_distribution_by_style table
    aggregated.saveToCassandra(KEYSPACE, PERFORMERS_DISTRIBUTION_BY_STYLE, SomeColumns("type","style","count"))

    sc.stop()
  }

  val PERFORMERS_TYPES = Map("Person"->"artist","Group"->"group")
}

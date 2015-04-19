package us.unemployment.demo


import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import us.unemployment.demo.UsUnemploymentSchema.{TABLE, KEYSPACE}

object FromCSVCaseClassToCassandra {

  /*
  Year,
  Civilian noninstitutional population,
  Civilian labor force (Total),
  % of Population,
  Employed Total,
  Employed % of Population,
  (of which) Agriculture,
  (of which) Non-Agriculture,
  Unemployed (Number),
  Unemployed % of labor force,
  Not in labor force,
  Footnotes
   */
  val CSV: String = "src/main/data/us_unemployment.csv"

  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("write_csv_to_cassandra")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    UsUnemploymentSchema.prepareSchemaAndCleanData(conf)

    val caseClassRDD: RDD[UsUnemployment] = sc.textFile(CSV)
      .zipWithIndex()
      .filter { case (line, index) => index > 0}
      .map { case (line, index) => {
        val lines = line.split(",")

        UsUnemployment(lines(0).toInt, lines(1).toInt, lines(2).toInt, lines(3).toDouble,
          lines(4).toInt, lines(5).toDouble, lines(6).toInt, lines(7).toInt,
          lines(8).toInt, lines(9).toDouble, lines(10).toInt, "")
      }
    }

    caseClassRDD.saveToCassandra(KEYSPACE, TABLE)

    sc.stop()
  }
}

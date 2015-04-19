package us.unemployment.demo


import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.spark.{SparkConf, SparkContext}
import us.unemployment.demo.UsUnemploymentSchema.{TABLE, KEYSPACE}

object FromCSVToCassandra {

  /*
    CREATE TABLE spark.us_unemploymen_stats (
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
      footnotes text);
   */

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
  val TABLE_COLUMNS = SomeColumns("year", "civil_non_institutional_count", "civil_labor_count", "labor_population_percentage",
    "employed_count", "employed_percentage", "agriculture_part_count", "non_agriculture_part_count", "unemployed_count",
    "unemployed_percentage_to_labor", "not_labor_count", "footnotes")


  def main (args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("write_csv_to_cassandra")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    UsUnemploymentSchema.prepareSchemaAndCleanData(conf)

    sc.textFile(CSV)
      .zipWithIndex()
      .filter {case (line, index) => index > 0}
      .map{case (line,index) => {
        val lines = line.split(",")

      (lines(0).toInt, lines(1).toInt, lines(2).toInt, lines(3).toDouble,
        lines(4).toInt, lines(5).toDouble, lines(6).toInt, lines(7).toInt,
        lines(8).toInt, lines(9).toDouble, lines(10).toInt, "")
      }}
      .saveToCassandra(KEYSPACE, TABLE, TABLE_COLUMNS)

    sc.stop()
  }



}

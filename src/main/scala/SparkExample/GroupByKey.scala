package SparkExample

import org.apache.spark.sql.SparkSession

object GroupByKey {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("groupbyKey").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val x = sc.parallelize(Array(("USA", 1), ("USA", 2), ("India", 1), ("UK", 1), ("India", 4), ("India", 9), ("USA", 8), ("USA", 3), ("India", 4),
      ("UK", 6), ("UK", 9), ("UK", 5)), 3)

   val y = x.groupByKey().collect()

    for (i <- y) {
      println("the output :" +i._1 +"the value is :" +i._2)
    }



  }
}

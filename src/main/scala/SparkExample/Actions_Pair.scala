package SparkExample

import org.apache.spark.sql.SparkSession

object Actions_Pair {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("PairedValue").master("local[*]").getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )

    val wordsRDD = rdd.flatMap(_.split(" "))
    val pairRDD = wordsRDD.map(x=> (x,1))

    pairRDD.foreach(f=> println(f))

    println("Distinct====>")
    pairRDD.distinct().foreach(f=> println(f))


  }
}

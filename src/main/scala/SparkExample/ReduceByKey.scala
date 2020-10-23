package SparkExample

import org.apache.spark.sql.SparkSession

object ReduceByKey {

  def main(args:Array[String]):Unit ={

    val spark = SparkSession.builder().appName("ReduceByKey").master("local[*]").
      getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //reduceByKey

    val rby_in = sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1), ("b", 1)), 3)

    val rby_out = rby_in.reduceByKey(_+_)

    rby_out.foreach(f=> println(f))

    val data = Seq(("Project", 1),
      ("Gutenberg’s", 1),
      ("Alice’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1))

    val rbk_rdd = sc.parallelize(data)
    val rbk_rdd_1 = rbk_rdd.reduceByKey(_ + _)

    rbk_rdd_1.foreach(f=>println(f))


  }

}

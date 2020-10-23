package SparkExample

import org.apache.spark.sql.SparkSession

object EmptyRDD {

  def main(args:Array[String]) = {
    val spark = SparkSession.builder().appName("EmptyRDD")
      .master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val rdd = spark.sparkContext.emptyRDD
    val rddString = spark.sparkContext.emptyRDD[String]

    println(rdd)
    println(rddString)

    println("Num of partitions: " + rdd.getNumPartitions)

    val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    println(rdd2)
    println("Num of partitions: "+rdd2.getNumPartitions)


    type pairRDD= (String,Int)

    var resultRDD = spark.sparkContext.emptyRDD[pairRDD]



  }
}

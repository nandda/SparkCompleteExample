package SparkExample

import org.apache.spark.sql.SparkSession

object Parallelize  {

  def main(args:Array[String]) ={

    val spark = SparkSession.builder().master("local[*]").appName("Parallelize").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val RDD = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    val rddCollect = RDD.collect()

    println("Number of partitions :" +RDD.getNumPartitions)
    println("Actual Data is:"+rddCollect.toList)



  }



}

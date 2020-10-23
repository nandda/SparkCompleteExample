package DataFrame_SparkSQL_part_2.SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[*]")
    .set("spark.cores.max","2")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val sc = spark.sparkContext
}

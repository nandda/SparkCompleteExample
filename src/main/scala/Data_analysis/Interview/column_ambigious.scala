package Data_analysis.Interview

import org.apache.spark.sql.SparkSession

object column_ambigious extends App {

  val spark = SparkSession.builder().appName("Column_ambigious").master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  sc.setLogLevel("ERROR")

  val data =
    spark.read.json("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\Interview\\column_ambigious.json")
}


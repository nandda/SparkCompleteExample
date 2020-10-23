package Data_analysis

import org.apache.spark.sql.SparkSession

object Popular_Movies {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Popular Movie").master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data_rdd = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\ratings_small.csv")
    val my_movies = data_rdd.map(x=> x.split(",")).map(x=> (x(0),1))
    val rating = my_movies.reduceByKey(_+_).map(x=> x.swap).sortByKey(false)
    rating.collect().foreach(x=>println(x))



  }
}

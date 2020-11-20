package Data_analysis_2
/*
This is very useful site to understand the broadcast variables
https://www.javahelps.com/2019/03/spark-06-broadcast-variables.html
*/


import org.apache.spark.sql.SparkSession

import scala.io.Source

object BroadCastVariable_AVG_Movie_Rat {

  def mapToTuple(lines:String) :(Int,(Float,Int)) ={

    val fields = lines.split(',')
    return (fields(1).toInt,(fields(2).toFloat,1))
  }

  def loadMovieNames(): Map[Int,String] =  {

    var movieNames:Map[Int,String] =Map()
    val lines = Source.fromFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis_2\\movies.csv")
      .getLines().drop(1)
    for(i <- lines) {
      val fields =i.split(',')
      movieNames += (fields(0).toInt -> fields(1))
    }
    return movieNames
  }

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("BroadCastVariable").master("local[*]")
      .getOrCreate()

    val sc =spark.sparkContext
    sc.setLogLevel("ERROR")

    val names = sc.broadcast(loadMovieNames())

    var data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis_2\\ratings.csv")

    val header = data.first()

    data = data.filter(x=> x !=header)

    val result = data.map(mapToTuple)
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .map(x=> (x._1,(x._2._1/x._2._2)))
      .map(x => (names.value(x._1),x._2))
        .sortBy(_._2,false)
        .collect()
    result.foreach(x=>println(x))










}


}

package Data_analysis_2

import org.apache.spark.sql.SparkSession

object AverageMovieRating {

  def mapToTuple(line:String) : (Int,(Float,Int))= {
    val lines = line.split(",")
    return (lines(1).toInt,(lines(2).toFloat,1))
  }

  def main(Args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("AverageMovieCounter").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    var data =sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis_2\\ratings.csv")

    val header = data.first()

    data = data.filter(x=> x != header)

    //val result = data.map(mapToTuple)
    //result.foreach(x=>println(x))

    val result = data.map(mapToTuple)
      .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
      .map(x=> (x._1,x._2._1/x._2._2))
      .sortBy(_._2,false)
        .collect()


    result.foreach(x=>println(x))
  }


}

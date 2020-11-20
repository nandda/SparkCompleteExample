package Data_analysis_2

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoRegistrator
object MovieRatingCounter {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MovieRatingCounter").master("local[*]")

      .getOrCreate()


    val sc = spark.sparkContext




    sc.setLogLevel("Error")

   var data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis_2\\ratings.csv")

    val header = data.first()

    data = data.filter(x=> x != header)

    val result = data.map(x=> x.split(',')(2).toFloat).countByValue()


    result.toSeq.sorted.foreach(x=>println(x))
println("xxxxxxxxxxxxxxxxxxxx")

    val result_2 =data.map(x=>(x.split(',')(2).toFloat,1))
     .reduceByKey((x,y)=> x+y )
        .sortBy(_._1,false)
        .collect()

    result_2.foreach(x=>println(x))









  }
}

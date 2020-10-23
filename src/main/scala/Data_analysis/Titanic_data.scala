package Data_analysis

import org.apache.spark.sql.SparkSession

object Titanic_data {

  def main(args:Array[String]): Unit = {


    val spark = SparkSession.builder().appName("Titanic_data").master("local[*]").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\train.csv")
   // val data_split = data.filter(x=> if(x.toString().split(",").length >=6) true else false).map(line => line.split(","))
   val split = data.filter { x => {if(x.toString().split(",").length >= 6) true else false} }.map(line=>{line.toString().split(",")})


  //  data_key.foreach(x=>println(x))



  }
}

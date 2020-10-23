package Data_analysis

import org.apache.spark.sql.SparkSession

object Pokemon_data {

  case class pokemon(Number:Int,Name:String,Type_1:String,Type_2:String,Total:Int,HP:Int,Attack:Int,Defense:Int,Sp_Atk:Int,Sp_Def:Int,Speed:Int)
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Pokemon_data").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
import spark.implicits._

    val data_pok = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\pokeman_data.csv")
      .map(x=>x.split(","))
      .map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10))).toDF()

    val data_df = data_pok.createOrReplaceTempView("data_pok")

 //   spark.sql("select avg(hp) from data_pok").show()
  }
}

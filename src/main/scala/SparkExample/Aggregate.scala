package SparkExample

import org.apache.spark.sql.SparkSession

object Aggregate {
def main(args:Array[String]): Unit = {

  val spark = SparkSession.builder().appName("Aggregate").master("local[*]").getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

  def param0 = (accum:Int,v:Int) => accum + v;
  def param1 = (accum1:Int ,accum2:Int) => accum1 + accum2

  val zero_val =0

  println("the aggregate value is :" +listRdd.aggregate(0)(param0,param1))

  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

  def param2 = (accum:Int,v:(String,Int)) => accum + v._2;
  def param3 = (accum1:Int ,accum2:Int) => accum1 + accum2

  println("the aggregate value is :" +inputRDD.aggregate(0)(param2,param3))




}
}

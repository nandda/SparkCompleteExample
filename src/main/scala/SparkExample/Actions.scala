package SparkExample

import org.apache.spark.sql.SparkSession

object Actions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Actions").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext
    //Reduce using seq of 1 to 10
    val x = sc.parallelize((1 to 10),3)
    val y = x.reduce((a,b)=>(a+b));
    println(y)

    val listRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    println("output min using binary" +listRDD.reduce((a,b) =>  a min b))
    println("output max using binary" +listRDD.reduce((a,b) => a max b))
    println("output sum using binary" +listRDD.reduce((a,b) => a+b))

    val inputRDD = sc.parallelize(List(("z",1),("A", 20),("B", 30),
      ("C", 40),("B", 30),("B", 60)))

    println("output sum using pair: "+inputRDD.reduce((a,b) => ("sum",a._2+b._2)) )
    println("output min using pair: "+inputRDD.reduce((a,b) => ("min",a._2 min b._2)))
    println("output max using pair: "+inputRDD.reduce((a,b)=>("max",a._2 max b._2)))


  }
}
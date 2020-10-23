package SparkExample

import org.apache.spark.sql.SparkSession

object Reduce {
 def main(args:Array[String]): Unit = {

   val spark = SparkSession.builder().appName("Reduce").master("local[*]").getOrCreate()
   val sc = spark.sparkContext

   sc.setLogLevel("ERROR")

   val rdd = sc.parallelize(Array(1.0,2,3,4,5,6,7),3)
   val rdd_map = rdd.map(x=> (x,1))

   val (sum,count) = rdd_map.reduce((a,b) => (a._1+b._1,a._2+b._2))
   val output = sum/count
   println(output)

   val rby_in = sc.parallelize(Array(("a", 2), ("b", 1), ("a", 1), ("a", 1), ("b", 2), ("b", 1), ("b", 1), ("b", 1)), 3)

   val output_1 = rby_in.reduceByKey(_+_)

   output_1.foreach(f=> println(f))




 }

}

package SparkExample

import org.apache.spark.sql.SparkSession

object GroupBy {
def main(args:Array[String]): Unit = {

  val spark = SparkSession.builder().appName("GroupBy").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  val x = sc.parallelize(Array("Joseph", "Jimmy", "Tina","Thomas", "James", "Cory", "Christine", "Jackeline", "Juan"), 3)

  val y = x.groupBy(x=> x.charAt(0))

  y.foreach(f=>(println(f)))



}
}

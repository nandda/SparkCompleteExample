package SparkExample


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CreateRDD {

  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    val columns = Seq("language","users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)

    val dfFromRDD1 = rdd.toDF("language","user_count")
    dfFromRDD1.printSchema()

    val columns1 = Seq("language","user_count")
    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns1:_*)


  }
}
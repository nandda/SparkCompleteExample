package SparkExample

import org.apache.spark.sql.SparkSession

object Transformstion_WordCount {

  def main(args:Array[String]) = {

    val spark = SparkSession.builder().appName("WordCount_with_Transformation").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val RDD_1 = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\test.txt")

    println("initial partition count:"+RDD_1.getNumPartitions)
    println("Repartition:" +RDD_1.repartition(4))
    println("initial partition count:"+RDD_1.getNumPartitions)

    val RDD_2=RDD_1.flatMap(x=> {x.split(" ")}).map(x=> {(x,1)})

    val RDD_3 = RDD_2.reduceByKey((a,b)=> a+b)

    val RDD_4 = RDD_3.map(x=> (x._2,x._1)).sortByKey(false)

    RDD_4.foreach(f=> println(f))

    println("Total Count :" +RDD_4.count())

    val RDD_5 =RDD_4.first()
    println("First Record:"+RDD_5._1+", "+RDD_5)

    val dataMax = RDD_4.max()
    println("The max value is:" +dataMax)





  }

}

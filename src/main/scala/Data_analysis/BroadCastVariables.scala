package Data_analysis

import org.apache.spark.sql.SparkSession

object BroadCastVariables extends App{

  val spark = SparkSession.builder().appName("BroadCast").master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")


  val lookup = Map("This"->"frequent","is"->"frequent","my"-> "moderate","file"->"rare")
  val broadcastV = sc.broadcast(lookup)

  val rdd =  Seq("this is is a","is a gossib","this is introvert","is is a rare file")

/*  def lookupWord(word:String) = (broadcastV.value.get(word).getOrElse("NA"))

  val rdd2 = rdd.flatMap(x => x.split(" ")).map(x => lookupWord(x))


  val myRDD = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\word_count_file")

  val myRDD2 = myRDD.flatMap(line => if(line != "") line.split(" ") else Array[String]())
      .map(lookupWord).countByValue()
  println("Total Blank Lines in the dataset file is: " +myRDD2.values)*/

val black_line_accumulator =sc.accumulator(0,"black lines")



  val input_file_black_line_count = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\word_count_file")

  input_file_black_line_count.foreach(x=> if(x.length ==0 ) black_line_accumulator +=1)

  println(black_line_accumulator.value)



}

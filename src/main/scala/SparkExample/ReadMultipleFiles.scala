package SparkExample

import org.apache.spark.sql.SparkSession

object ReadMultipleFiles {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("ReadMultipleText").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    println("Read Multiple files from a Directory")
    val rdd_text = sc.textFile("C:\\Users\\nanda\\Documents\\CloudxLab\\files\\*")
    // rdd_text.foreach(f => println(f))

    println("Read text files based on wildcard ")
    val rdd_text2 = sc.textFile("C:\\Users\\nanda\\Documents\\CloudxLab\\files\\*.txt")
    rdd_text2.foreach(f => println(f))

    println("Read text files based on wholetextFiles:")
    val rdd_text3 = sc.wholeTextFiles("C:\\Users\\nanda\\Documents\\CloudxLab\\files\\*")
    rdd_text3.foreach(f => println(f))

    println("Read the text Files CSV")
    val rdd_text4 = sc.textFile("C:\\Users\\nanda\\Documents\\CloudxLab\\files\\*")
    val rdd_split = rdd_text4.map(x=> (x.split(",")))
    rdd_split.foreach(f=> {println("col1:" +f(0)+"col2"+f(1))})


  }
}

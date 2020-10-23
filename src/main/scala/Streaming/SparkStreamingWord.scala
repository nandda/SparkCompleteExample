package Streaming
//Socket//
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkStreamingWord {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Streaming").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(2))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(x => x.split(" "))

    val pairs = words.map(x => (x, 1))

    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
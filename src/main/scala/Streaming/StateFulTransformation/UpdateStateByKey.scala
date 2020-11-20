package Streaming.StateFulTransformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKey {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("UpdateStateByKey").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc,Seconds(5))

    ssc.checkpoint("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\StateFulTransformation\\")

    val line = ssc.socketTextStream("localhost",9999)

    val words = line.map((_,1))

    val wordCount = words.updateStateByKey((values:Seq[Int],state:Option[Int]) => {
      val newValue =state.getOrElse(0) + values.sum
      Some(newValue)
    })

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

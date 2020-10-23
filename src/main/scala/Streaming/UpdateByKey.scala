package Streaming

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, Seconds, State, StateSpec, StreamingContext, Time}

/**
 * This is a minimum example for word count using Spark streaming.
 * The input stream is simply just one word per line from a folder
 * which may contains multiple files and new files may be added to
 * the folder as the updated data as a stream source
 */

object UpdateByKey {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("UpdateByKey").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc,Seconds(10))

    ssc.checkpoint("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\INPUT_FILES")

    val lines = ssc.textFileStream("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\Streaming_input")

    def updateFunction(newData: Seq[Long], state : Option[Long]) = {
      val newState = state.getOrElse(0L) + newData.sum
      Some(newState)
    }
/*
    def updateFunction2(iterator: Iterator[(String,Seq[Long],Option[Long])]): Unit = {
      iterator.toIterable.map {
        case (key,values,state) => (key,state.getOrElse(0L) + values.sum)
      }.toIterator
    }*/


    def mappingFunction(key:String,value:Option[Long],state:State[Long]) = {
      value match {
        case Some(v) => {
          state.update(state.getOption().getOrElse(0L) + v)
          (key,state.get())
        }
        case _ => (key,0L)
      }
    }
//    val r = lines.map( line => (line, 1L) ).reduceByKey(_ + _).updateStateByKey(updateFunction2 _,new HashPartitioner(8),false)
 /*   val spec = StateSpec.function(mappingFunction _).timeout(Durations.seconds(2))
    val r = lines.map( line => (line, 1L) ).reduceByKey(_ + _).mapWithState(spec)*/

 val r = lines.map( line => (line, 1L) ).reduceByKey(_ + _).updateStateByKey(updateFunction)
    r.print()

//save to file
    r.foreachRDD { (rdd: RDD[(String, Long)], time: Time) =>
      println(">>>Time: " + time + " Number of partitions: " + rdd.getNumPartitions)
      rdd.saveAsTextFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\INPUT_FILES")
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

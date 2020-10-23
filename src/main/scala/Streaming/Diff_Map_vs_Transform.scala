package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue


//Please refer the given link
/*
https://stackoverflow.com/questions/32168039/what-is-exact-difference-between-spark-transform-in-dstream-and-map
 */

object Diff_Map_vs_Transform extends App{

/*  map(func) : Return a new DStream by passing each element of the
  source DStream through a function func.*/

  val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingTransformExample")
  val ssc = new StreamingContext(conf, Seconds(5))

  val rdd1 = ssc.sparkContext.parallelize(Array(1,2,3))
  val rdd2 = ssc.sparkContext.parallelize(Array(4,5,6))
  val rddQueue = new Queue[RDD[Int]]
  rddQueue.enqueue(rdd1)
  rddQueue.enqueue(rdd2)

  val numsDStream = ssc.queueStream(rddQueue, true)
  val plusOneDStream = numsDStream.map(x => x+1)
  plusOneDStream.foreachRDD(x=> println(x))


/*  transform(func) :
 Return a new DStream by applying a RDD-to-RDD function to every RDD of the source DStream.
 This can be used to do arbitrary RDD operations on the DStream.*/

  val commonRdd = ssc.sparkContext.parallelize(Array(0))
  val combinedDStream = numsDStream.transform(rdd=>(rdd.union(commonRdd)))
  combinedDStream.print()


}

package Streaming.StateFulTransformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, State, StateSpec, StreamingContext}

object TestMapWithState {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName(s"${this.getClass.getSimpleName}").master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc,Seconds(3))
    ssc.checkpoint("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\StateFulTransformation\\")

    val line =ssc.socketTextStream("localhost",9999)
    val wordStream = line.flatMap(_.split(",")).map(x=>(x,1))

    val mappingFunc = (userId:String,value:Option[Int],state:State[Int]) => {
      val sum = value.getOrElse(0) + state.getOption().getOrElse(0)
      val output = (userId,sum)
      state.update(sum)
      output
    }

    val stateDstream = wordStream.mapWithState(StateSpec.function(mappingFunc).timeout(Minutes(60)))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

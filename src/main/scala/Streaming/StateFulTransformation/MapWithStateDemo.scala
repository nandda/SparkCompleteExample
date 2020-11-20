package Streaming.StateFulTransformation

//https://blog.csdn.net/An1090239782/article/details/102832444#22mapWithStateScala_22

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object MapWithStateDemo {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MapWithStateDemo").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\StateFulTransformation\\")

    val fileDS = ssc.socketTextStream("localhost",9999)

    val wordStream = fileDS.flatMap{line =>line.split("\t")}
      .map{word => (word,1)}

    val mappingFunc = (word:String,one:Option[Int],state:State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
      val output = (word,sum)
      state.update(sum)
      output
    }

    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    val stateDstream = wordStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()

  }
}

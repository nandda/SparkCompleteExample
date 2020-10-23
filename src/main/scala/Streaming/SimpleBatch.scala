/*
package Streaming
import Streaming.Person
import org.apache.spark.sql.{Encoders, SparkSession}

object SimpleBatch {
val INPUT_FILE:String = "D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\people_1.json"


def main(args:Array[String]): Unit = {
  SimpleBatch.startJob()

}
  def startJob(): Unit = {
    val startTime :Long = System.currentTimeMillis()

    val spark = SparkSession.builder().master("local[*]").appName("SimpleBatch")
      .config("spark.eventLog.enabled","false")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","2g")
      .getOrCreate()


val  sourceDS = spark.read.json(INPUT_FILE).as(Encoders.bean(Person.getClass))

    sourceDS.createOrReplaceTempView("people")

    spark.sql("select sex from people limit 10").show()

  }
}
*/



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders
/*import ca.redsofa.domain.Person
import ca.redsofa.udf.StringLengthUdf*/
import org.apache.spark.sql.functions.callUDF


object SimpleBatch {
  private val INPUT_FILE = "D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\people_1.json"

  case class Person_2(firstName:String,lastName:String,sex:String,age:Long)

  def main(args:Array[String]): Unit = {
    System.out.println("Stating Job...")

    val startTime = System.currentTimeMillis

    val spark =SparkSession
      .builder()
      .appName("Simple Batch Job")
      .master("local[*]")
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .getOrCreate();

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
  val SourceDS = spark.read.json(INPUT_FILE).as(Encoders.bean(classOf[Person_2]))

    SourceDS.createOrReplaceTempView("people")

    spark.sql("select * from people").show(10)

    spark.stop()
    val stopTime:Long =System.currentTimeMillis()
    val elapsedTime:Long = stopTime-startTime
    System.out.println("Execution time in ms : " + elapsedTime)
  }


}
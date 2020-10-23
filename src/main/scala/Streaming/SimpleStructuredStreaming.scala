package Streaming


import java.util.Properties

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types.StructType

object SimpleStructuredStreaming {

  case class Person_2(firstName:String,lastName:String,sex:String,age:Long)

  //val INPUT_FILE:String = "D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\INPUT_FILES"
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SimpleStructureStreaming").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val personSchema = new StructType()
      .add("firstName","string")
      .add("lastName","string")
      .add("sex",dataType = "string")
      .add("age","long")

    val personStream = spark.readStream.schema(personSchema).json("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Streaming\\INPUT_FILES")
      .as(Encoders.bean(classOf[Person_2]))

    personStream.createOrReplaceTempView("people")

    val query = spark.sql("select avg(age) as avg_age,sex from people group by sex")


    val url = "jdbc:mysql://localhost:3306"
 /*   val properties = new Properties()
      properties.put("user","root")
      properties.put("password","root")
      Class.forName("com.mysql.jdbc.Driver")

    val table = "demo.test"*/

    val store = query.writeStream.outputMode("complete").format("console")
      .start()


    store.awaitTermination()






  }
}

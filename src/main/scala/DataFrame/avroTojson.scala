package DataFrame

import org.apache.spark.sql.{SparkSession,SaveMode}

object avroTojson {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("AvroToJson").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val df = spark.read.format("avro")
      .load("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\zipcodes.avro")

    df.show()
    df.printSchema()

    df.write.mode(SaveMode.Overwrite)
      .json("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\zipcodes.json")





  }
}

package Streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkStreamingDirectory {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SparkStreamingDirectory").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val schema = StructType(
      List(
        StructField("RecordNumber", IntegerType, true),
        StructField("Zipcode", StringType, true),
        StructField("ZipCodeType", StringType, true),
        StructField("City", StringType, true),
        StructField("State", StringType, true),
        StructField("LocationType", StringType, true),
        StructField("Lat", StringType, true),
        StructField("Long", StringType, true),
        StructField("Xaxis", StringType, true),
        StructField("Yaxis", StringType, true),
        StructField("Zaxis", StringType, true),
        StructField("WorldRegion", StringType, true),
        StructField("Country", StringType, true),
        StructField("LocationText", StringType, true),
        StructField("Location", StringType, true),
        StructField("Decommisioned", StringType, true)
      )
    )

    val df = spark.readStream.schema(schema)
      .json("C:\\Users\\nanda\\Documents\\CloudxLab\\files\\StreamingFiles")

    df.printSchema()

    val groupDF = df.select("Zipcode")
      .groupBy("Zipcode").count()
    groupDF.printSchema()


    groupDF.writeStream.format("console")
      .outputMode("complete")
      .option("truncate", false)
      .option("newRows", 30)
      .start()
      .awaitTermination()

    groupDF.writeStream.format("jdbc")
      .option("url","jdbc:mysql://localhost:50000/dbname")
      .option("driver","com.")
  }
}
package DataFrame

import org.apache.spark.sql.SparkSession

object CsvToAvroParquetJson {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CsvToAvro").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

  /*val df = spark.read.options(Map("InferSchema" ->"true","delimiter" ->",","header"->"true"))
    .csv("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\zipcodes.csv")

    df.show(false)*/

    val with_schema = spark.read.format("csv")
      .option("header","true")
      .load("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\zipcodes.csv")

    with_schema.show(false)


  }
}

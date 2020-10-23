package Data_analysis.Interview

import org.apache.spark.sql.SparkSession
object multiple_delimiter extends  App {


  val spark = SparkSession.builder().appName("Multiple_delimiter").master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val data = spark.read.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\scenario.csv")
import spark.implicits._
  val data_1 = data.map(x=> x.split("\\~\\|").mkString("\t"))
  val data_2 = spark.read.option("delimiter","\t").option("header",true).csv(data_1)

  data_2.show()

}

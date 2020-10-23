package Data_analysis.Interview

import org.apache.spark.sql.SparkSession

object scenario_2_skip_line extends App {

  val spark = SparkSession.builder().appName("Skip unwanted line").master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val data =
    sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\pageView.csv",2)

  val data_1 = data.mapPartitionsWithIndex((idx,itr) => if (idx==0) itr.drop(8) else itr)

  import spark.implicits._
  val data_2 = data_1.map(x=>x.split(",")).map(x=> (x(0),x(1),x(2),x(3),x(4))).toDF()


  data_2.show(truncate = false)
  //data_2.foreach(x=>println(x))

}

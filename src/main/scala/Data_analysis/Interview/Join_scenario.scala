package Data_analysis.Interview

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.LeftSemi


object Join_scenario extends App{

  val spark = SparkSession.builder().appName("Join_case").master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")


  val data_1 = spark.read .option("delimiter","|")
    .option("header",true)
    .csv("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\Interview\\Master_rec")

  //data_1.show()

  val data_2 = spark.read .option("delimiter","|")
    .option("header",true)
    .csv("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\Interview\\details_rec")

  data_1.intersect(data_2).show()
  data_1.intersectAll(data_2).show()
  println("Intersect Finished---------->")

data_1.join(data_2,data_1("Roll_no") ===data_2("Roll_no"),"LeftSemi")
    .show()

  data_1.join(data_2,data_1("Roll_no") ===data_2("Roll_no"),"LeftAnti")
    .show()

}

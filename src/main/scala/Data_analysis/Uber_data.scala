package Data_analysis

import org.apache.spark.sql.SparkSession

object Uber_data {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Uber_Data").master("local[*]").getOrCreate()
    val sc= spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\Uber_Data.csv")
    val data_first = data.first()

    val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
    val day = Array("Sun","Mon","Tue","Wed","Thu","Fri","Sat")

    val eliminate = data.filter(x=> x != data_first)

    val split = eliminate.map(x=> x.split(",")).map(x=> (x(0),format.parse(x(1)),x(3)))
    val combine = split.map(x=> (x._1+" "+day(x._2.getDay),x._3.toInt))
    val arrange = combine.reduceByKey(_+_).map(x=> x.swap).collect().foreach(x=> println(x))




  }
}

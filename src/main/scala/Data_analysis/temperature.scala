package Data_analysis

import org.apache.spark.sql.SparkSession

object temperature {

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Temperature").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\Weather_data")

    def parsed_line(line:String): (String, String, Double) = {
      val fields = line.split(",")
      val stationID = fields(0)
      val entryType = fields(2)
      val temperature = fields(3).toDouble * 0.1  * (9.0/5.0) +32.0
  (stationID,entryType,temperature)

    }

  val parsed_res = data.map(x=> parsed_line(x))
   // parsed_res.foreach(x=>println(x))
val tmin_filtered = parsed_res.filter(x=> x._2.contains("TMAX")).map(x=> (x._2,x._3))

    val agg_data = tmin_filtered.reduceByKey((a,b) => (a max b)).foreach(x=>print(x))


  }
}

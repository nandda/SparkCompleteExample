package Data_analysis

import org.apache.spark.sql.SparkSession

object IPL_Data {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("IPL DATA ANALYSIS").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data_df = spark.read.format("csv")
      .option("header", true)
      .option("delimited", ",")
      .load("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\matches.csv")

   //data_df.show(truncate = false)

   data_df.createOrReplaceTempView("ipl_matches")

/*
 val data_bat_first=   spark.sql("select a.venue,a.cnt,b.cnt, ((a.cnt*100)/b.cnt) as bat_won_per from ((select venue,count(1) as cnt from ipl_matches where win_by_runs != 0 " +
      " group by venue order by cnt desc) a inner join" +
   "(select venue,count(1) as cnt from ipl_matches  group by venue) b " +
   "on (a.venue = b.venue)) ")
      .show(200,truncate = false)
  //  val data_total_mactch = spark.sql("select venue,count(1) as cnt from ipl_matches where venue = 'Wankhede Stadium' group by venue").show()
*/



    val data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\matches.csv")
    val data_map = data.map(x=> x.split(",")).filter(x=>x.length<19)
    val data_map_filter = data_map.map(x=> (x(7),x(11),x(12),x(14)))
    val bat_first_won  = data_map_filter.filter(x=> x._2 !="0").map(x=> (x._4,1)).reduceByKey(_+_).sortByKey(false)
    /*bat_first_won.collect().foreach(x=>println(x))*/

    //data_map_filter.foreach(f=>println(f))
val venue_tot = data_map_filter.map(x=> (x._4,1)).reduceByKey(_+_).sortByKey(false)


  bat_first_won.join(venue_tot).map(x=>(x._1,(x._2._1*100)/x._2._2)).map(item=> item.swap).sortByKey(false)
      .collect().foreach(x=>println(x))







  }
}
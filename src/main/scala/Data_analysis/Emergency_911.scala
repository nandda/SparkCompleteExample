package Data_analysis

import org.apache.spark.sql.SparkSession

object Emergency_911 {

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Emergency_calls").master("local[*]")
      .getOrCreate()

    val sc =spark.sparkContext
    sc.setLogLevel("ERROR")
   case class emergency(lat:String,lng:String,desc:String,zip:String,title:String,timeStamp:String,twp:String,addr:String,e:String)

    val data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\911.csv")
    val header = data.first()
    val data1 = data.filter(x=> (x != header))
    import spark.implicits._
   /* val data_split = data.map(x=>x.split(",")).filter(x => x.length>=9).map(x => emergency(x(0),x(1),x(2),x(3),x(4)
    ,x(5),x(6),x(7),x(8))).toDF("lat","lng","desc","zip","title","timeStamp","twp","addr","e")*/

    val emergency_data = data1.map(x=>x.split(",")).filter(x => x.length>=9).map(x => (x(0),x(1),x(2),x(3),x(4)
    ,x(5),x(6),x(7),x(8))).toDF("lat","lng","desc","zip","title","timeStamp","twp","addr","e")
    emergency_data.createOrReplaceTempView("emergency_911")

   val data_emp = spark.sql("select * from emergency_911")


    val data_2 = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\free-zipcode-database.csv")
  // data_2.take(10).foreach(x=>println(x))
    val remove_header = data_2.first()
    val zipCode = data_2.filter(x=> x != remove_header)
    val zipCode_data = zipCode.map(x=> x.split(",")).map(x=> (x(1).replace("\"",""),x(3).replace("\"",""),x(4).replace("\"",""))).toDF("zipcode","city","state")

    zipCode_data.createOrReplaceTempView("ZipCode")

   val Zip_data =spark.sql("select * from ZipCode")

val build1 = spark.sql("select substring(e.title,0,instr(e.title,':')-1) as title ,z.city,z.state from  emergency_911 e inner join ZipCode z on e.zip = z.zipcode")


    val ps = build1.map(x => x(0)+" -->"+x(2).toString)
    val ps1 = ps.map(x=> (x,1)).rdd.reduceByKey(_+_).foreach(x=>println(x))
  }
}

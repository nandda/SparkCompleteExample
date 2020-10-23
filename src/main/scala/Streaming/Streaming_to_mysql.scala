import org.apache.spark.SparkConf
import org.apache.spark.streaming
import org.apache.spark.streaming._
import java.sql.{Connection,DriverManager,PreparedStatement}
import org.apache.spark.streaming.Seconds

object Streaming_to_mysql {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("streaming")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(1))

    val dataStream = ssc.socketTextStream("localhost",9999)


    val pairs = dataStream.map(line=>(line.split(",")(0),1))
      .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(2))


    val hotword = pairs.transform(rdd =>{
      val top3 = rdd.map(pair=>(pair._2,pair._1)).sortByKey(false).take(3)
      ssc.sparkContext.makeRDD(top3)
    })



    hotword.foreachRDD(rdd=>{rdd.foreachPartition(partionOfRecords=>{
      val url = "jdbc:mysql://localhost:3306/spark"
      val user = "root"
      val password = "root"
      Class.forName("com.mysql.cj.jdbc.Driver")
      val conn = DriverManager.getConnection(url,user,password)
      conn.prepareStatement("delete from searchKeyWord where 1=1").executeLargeUpdate()
      conn.setAutoCommit(false)
      val stmt = conn.createStatement()
      partionOfRecords.foreach(record=>{
        stmt.addBatch("insert into searchKeyWord(insert_time,keyword,search_count) values(now(),'"+record._1+"','"+record._2+"')")
      })
      stmt.executeBatch()
      conn.commit()
    })
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
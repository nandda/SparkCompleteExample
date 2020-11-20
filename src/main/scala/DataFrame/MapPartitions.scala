package DataFrame

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class Util extends Serializable {
  def combine(fname:String,mname:String,lname:String): String = {
    fname+","+mname+","+lname
  }
}

object MapPartitions {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]")
      .appName("MapPartitions").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val structureData = Seq(
      Row("James","","Smith","36636","NewYork",3100),
      Row("Michael","Rose","","40288","California",4300),
      Row("Robert","","Williams","42114","Florida",1400),
      Row("Maria","Anne","Jones","39192","Florida",5500),
      Row("Jen","Mary","Brown","34561","NewYork",3000)
    )

    val structureSchema = new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("id", StringType)
      .add("location", StringType)
      .add("salary", IntegerType)


    val df2 =spark.createDataFrame(sc.parallelize(structureData),structureSchema)
    df2.printSchema()
    df2.show(false)

    import spark.implicits._
    val util =new Util
    val df3 = df2.map(row => {

      val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
      (fullName,row.getAs[String](3),row.getAs[String](4),row.getAs[Int](5))
    })

    val df3Map = df3.toDF("fullName","id","location","salary")
    df3Map.printSchema()
    df3Map.show(false)


    val df4 = df2.mapPartitions(iterator => {
      val util = new Util()
      val res = iterator.map(row=>{
        val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
        (fullName, row.getString(3),row.getInt(5))
      })
      res
    })
    val df4part = df4.toDF("fullName","id","salary")
    df4part.printSchema()
    df4part.show(false)
  }



}

package DataFrame

import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object Avro_read_write {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkExamples").master("local[*]").getOrCreate()
    val sc =spark.sparkContext
    sc.setLogLevel("ERROR")
    val data = Seq(("James ", "", "Smith", 2018, 1, "M", 3000),
      ("Michael ", "Rose", "", 2010, 3, "M", 4000),
      ("Robert ", "", "Williams", 2010, 3, "M", 4000),
      ("Maria ", "Anne", "Jones", 2005, 5, "F", 4000),
      ("Jen", "Mary", "Brown", 2010, 7, "", -1)
    )

 //   val df = spark.createDataFrame(data).toDF("Fname","Mname","Lname","Year","dob","Gender","Salary")

    val columns = Seq("Fname","Mname","Lname","Year","dob","Gender","Salary")
    val df = spark.createDataFrame(data).toDF()
   //    df.show(false)

   /* df.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\person.avro")*/

 /*   spark.read.format("avro")
      .load("D:\\\\Development\\\\xxxplatform\\\\SparkExample\\\\src\\\\main\\\\scala\\\\DataFrame\\\\person.avro").show()*/

   /*df.write.partitionBy("year","dob")
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\person.avro")
  */


 /* spark.read.format("avro")
      .load("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\person.avro")
      .where(col("year")===2010)
      .show()*/

 //schema parser

/*    val schemaAvro = new Schema.Parser()
      .parse(new File("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\person.avsc"))


    spark.read.format("avro")
      .option("avroSchema",schemaAvro.toString())
      .load("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame\\person.avro")
      .show(false)*/


    spark.sqlContext.sql("CREATE TEMPORARY VIEW PERSON USING avro OPTIONS (path \"D:/Development/xxxplatform/SparkExample/src/main/scala/DataFrame/person.avro\")")
    spark.sqlContext.sql("select * from PERSON").show()
  }

}

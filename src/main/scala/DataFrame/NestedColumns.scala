package DataFrame

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object NestedColumns {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("NestedColumns").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val schema = new StructType()
      .add("name",new StructType()
      .add("FirstName",StringType)
      .add("MiddleName",StringType)
      .add("LastName",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val df = spark.createDataFrame(sc.parallelize(data),schema).toDF()
   df.printSchema()

    df.select(col("name.FirstName").as("Fname"),
      col("name.MiddleName").as("Mname"),
      col("name.LastName").as("Lname"),
      col("dob"),
      col("gender"),
      col("salary")).show(false)
  }

}

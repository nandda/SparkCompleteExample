package DataFrame

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions._

object Filter_where {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Where_and_Filter").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val arrayStructureData = Seq(
      Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
      Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
      Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
      Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
      Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
      Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
    )

    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("languages", ArrayType(StringType))
      .add("state", StringType)
      .add("gender", StringType)

   import spark.implicits._
    val df = spark.createDataFrame(sc.parallelize(arrayStructureData),arrayStructureSchema).toDF()
    //df.printSchema()
   // df.show(false)

    //condition
    df.filter(df("state") ==="OH")
      //.show(false)

    //SQL Expression
    df.filter("gender =='M'")
      //.show(false)

    //multiple condition
    df.filter(df("state")==="OH" && df("gender") === "M")
      //.show(false)

    //Array condition
    df.filter(array_contains(df("languages"),"Java"))
    //    .show(false)

    //structure condition
    df.filter(df("name.lastname")==="Williams")
      //.show(false)

    val df3 = df.select(col("*"),when(col("gender")==="M","Male")
    .when(col("gender")==="F","Female")
    .otherwise("Unknown").alias("new_gender"))







  }

}

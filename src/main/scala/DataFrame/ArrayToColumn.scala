package DataFrame

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ArrayToColumn {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkExamples").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

/*    val arrayData = Seq(
      Row("James", List("Java", "Scala", "C++")),
      Row("Michael", List("Spark", "Java", "C++")),
      Row("Robert", List("CSharp", "VB", ""))
    )*/

    val arraySchema = new StructType()
      .add("names", StringType)
      .add("Languages", ArrayType(ArrayType(StringType)))


 // val df = spark.createDataFrame(sc.parallelize(arrayData),arraySchema)
   // df.show(false)

    val arrayArrayData = Seq(
      Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
      Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
      Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
    )

    val df = spark.createDataFrame(sc.parallelize(arrayArrayData),arraySchema)
    df.show(false)

   val df_2 = df.select(df("names")
   +: (0 until 2).map(i=> df("Languages")(i).alias("subject")):_*)
   df_2.show(false)

  }
}

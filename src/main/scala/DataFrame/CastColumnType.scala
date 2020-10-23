package DataFrame

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CastColumnType {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("castcolumn").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val simpleData = Seq(Row("James", 34, "2006-01-01", "true", "M", 3000.60),
      Row("Michael", 33, "1980-01-10", "true", "F", 3300.80),
      Row("Robert", 37, "06-01-1992", "false", "M", 5000.50)
    )

    val columns = new StructType()
      .add("firstName", StringType)
      .add("age", IntegerType)
      .add("jobStartDate", StringType)
      .add("isGraduated", StringType)
      .add("gender", StringType)
      .add("salary", DoubleType)

    val df = spark.createDataFrame(sc.parallelize(simpleData), columns)

   // df.show()

    val df2 = df.withColumn("age",df("age").cast(StringType))
   // df2.printSchema()

    val df3 = df2.selectExpr("cast(age as int) age",
    "cast(isGraduated as string ) isGraduated")
    df3.printSchema()

    df2.show()

  }
}
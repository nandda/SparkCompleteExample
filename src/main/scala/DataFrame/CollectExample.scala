package DataFrame

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CollectExample {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CollectExample").master("local[*]")
      .getOrCreate()
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
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("id",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val df = spark.createDataFrame(sc.parallelize(data),schema)
    df.printSchema()
    df.show(false)


    val colData = df.collect()

/*    colData.foreach(row => {
      val salary = row.getAs[Int](3)
      println(salary)
    })*/



    colData.foreach(row => {
      val salary = row.getAs[Int](3)
      val fullName  = row.getStruct(0)
      val firstName = fullName.getAs[String](0)
      val middleName = fullName.getAs[String](1)
      val lastName = fullName.getAs[String](2)

      println(firstName+","+middleName+","+lastName+","+salary)


    })

  }

}

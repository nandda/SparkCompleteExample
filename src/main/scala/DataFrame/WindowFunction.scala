package DataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object WindowFunction {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("WindowFunction").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )

    val df = simpleData.toDF("employee_name","department","salary")
  //  df.show(false)

    //row number
    val windowSpec = Window.partitionBy("department").orderBy("salary")
    df.withColumn("row_number",row_number.over(windowSpec))
    .show(false)

    //rank
    df.withColumn("rank",rank().over(windowSpec))
      .show(false)

    //dense_rank
    df.withColumn("dense_rank",dense_rank().over(windowSpec))
      .show(false)

    //percent_rank
    df.withColumn("percent_rank",percent_rank().over(windowSpec))
      .show(false)

    //ntile
    df.withColumn("ntile",ntile(2).over(windowSpec))
        .show(false)
    //cume_dist
    df.withColumn("cume_dist",cume_dist().over(windowSpec))
        .show(false)
    //lag
    df.withColumn("lag",lag("salary",2).over(windowSpec))
        .show(false)
    //lead
    df.withColumn("lead",lead("salary",2).over(windowSpec))
      .show(false)


//    val windowSpec2 =Window.partitionBy("department").orderBy("salary")
  }

}

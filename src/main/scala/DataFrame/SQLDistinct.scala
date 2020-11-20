package DataFrame

import org.apache.spark.sql.SparkSession

object SQLDistinct {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Distinct")
      .getOrCreate()
    val sc =spark.sparkContext
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
    val columns = Seq("names","department","salary")
    val df = spark.createDataFrame(sc.parallelize(simpleData)).toDF(columns:_*)
    df.show(false)

    //Distinct all columns
    val distinctDF = df.distinct()
    println("Distinct count: "+distinctDF.count())

    distinctDF.show(false)

    //Distinct using dropDuplicates
     val dropDF = df.dropDuplicates("department","salary")
    println("Distinct count of department & salary :"+dropDF.count())
    dropDF.show(false)

  }

}

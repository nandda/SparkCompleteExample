package Data_analysis

import org.apache.spark.sql.SparkSession

object Soccer_data {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Soccer_data").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = spark.read.format("csv")
      .option("header",true)
      .option("delimited",",")
      .load("C:\\Users\\nanda\\Documents\\CloudxLab\\spark\\Soccer_Data_Set.docx.csv")

    data.createOrReplaceTempView("olympics")
  //  spark.sql("select * from olympics").show(false);

 /*   spark.sql("select count('Medal'),Country from olympics where Medal ='Bronze' and Sport = 'Football' group by Country")
      .show(false)*/

 /*   spark.sql("select count('Medal'),Country,Sport from olympics where Country ='USA' " +
      "group by Country,Sport").show(false)*/

   /* spark.sql("select count('Medal'),Country,Medal from olympics group by Country,Medal").show(false)*/

    spark.sql("select count('Medal') as cnt, Country,year from olympics " +
      "where Country ='MEX' and Medal ='Silver' group by Country,year").show(false)

   // spark.sql("select distinct Country from olympics").show(100,false)
  }
}

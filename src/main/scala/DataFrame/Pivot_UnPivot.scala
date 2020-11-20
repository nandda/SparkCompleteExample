package DataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Pivot_UnPivot {



  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Pivot").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    import spark.implicits._
    val df =data.toDF("Product","Amount","Country")
   // df.show(false)

    //pivotDF
   val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
     //.show(false)

    //unpivotDF
    val unPivotDF = pivotDF.select($"Product",
      expr("stack(4,'Canada',Canada,'China',China,'Mexico',Mexico,'USA',USA) as (Country,Total)"))
      .where("Total is not null")

      .show(false)





  }

}

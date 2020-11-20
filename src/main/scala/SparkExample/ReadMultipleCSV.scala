package SparkExample
import org.apache.spark.sql.SparkSession

object ReadMultipleCSV {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local[*]").appName("ReadCSVFile")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")



    val RDD_CSV = sc.textFile("C:\\Users\\nanda\\Documents\\CloudxLab\\files\\text01.csv")
    println(RDD_CSV.getClass)
    println(RDD_CSV.getNumPartitions)
   val rdd = RDD_CSV.map(f=> {f.split(",")})
   val rdd_skip = rdd.mapPartitionsWithIndex((idr,itr) => if(idr==0) itr.drop(1) else itr)
    rdd_skip.foreach(f=> println(f(0)+"  "+f(1)))

  }
}
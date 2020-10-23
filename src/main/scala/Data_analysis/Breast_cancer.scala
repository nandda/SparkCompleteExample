package Data_analysis

import org.apache.spark.sql.SparkSession

object Breast_cancer {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Breast_data").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\breast_cancer_clinical_data.csv")
    val header = data.first()
    val remove_header = data.filter(x=> x != header)

    //Avg age of people to do test
    val avg_age = remove_header.map(x=> x.split(",")).map(x=> x(2).toInt).reduce(_+_)/remove_header.count

    //println(avg_age)

    val stages = remove_header.map(x=> x.split(",")).map(x=> (x(11),x(15).toInt))
      .map(x=> (x._1,(x._2,1)))
      .reduceByKey((a,b) => (a._1+b._1,a._2+b._2))
      .map(x => (x._1,(1.0*x._2._1/x._2._2)))
      .foreach(x=>println(x))




  }
}

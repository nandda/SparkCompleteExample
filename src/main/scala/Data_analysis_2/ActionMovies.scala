package Data_analysis_2

import org.apache.spark.sql.SparkSession

/* please refer this materials to understand much more clear manner
https://www.javahelps.com/search?q=spark
 */

object ActionMovies {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ActionMovies_FlatMap").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    var data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis_2\\movies.csv")

    val header = data.first()

    data = data.filter(x=> x != header)

  /*  val result = data.map(x=> x.split(","))
      .map(fields => (fields(1),fields(2)))
        .flatMapValues(x=> x.split('|'))
        .filter(x=> x._2 =="Action")
        .map(x=> x._1)
        .sortBy(x=> x,true)

    result.foreach(x=> println(x))*/

    val result = data.map(x=> x.split(','))
      .map(fields => (fields(1),fields(2)))
      .flatMapValues(x=> x.split('|'))
        .filter(x=> (x._2 =="Action"))
        .map(x=> (x._1))
        .sortBy(x=> x,true)

    result.foreach(x=>println(x))

  }
}

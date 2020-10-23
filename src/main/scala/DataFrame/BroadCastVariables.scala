package DataFrame

import org.apache.spark.sql.SparkSession

object BroadCastVariables {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("BroadCast").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries = Map(("USA","United States of America"),("IN","India"))

    val broadcaststates = spark.sparkContext.broadcast(states)
    val broadcastcountries = spark.sparkContext.broadcast(countries)

    val data = Seq(("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    )


    val columns = Seq("FirstName","LastName","Country","States")

    import spark.implicits._
    val df = data.toDF(columns:_*)

   // df.show()

    val df2 = df.map(row => {
      val country = row.getString(2)
      val states = row.getString(3)

      val fullCountry = broadcastcountries.value.get(country).get
      val fullStates = broadcaststates.value.get(states).get
      (row.getString(0),row.getString(1),fullCountry,fullStates)
    })

    df2.toDF(columns:_*).show()



  }
}

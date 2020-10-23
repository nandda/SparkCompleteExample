package Data_analysis

import org.apache.spark.sql.SparkSession

object HospitalPatientAnalysis {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Hospital_data").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = spark.read.format("csv")
      .option("inferSchema",true)
      .option("Header",true)
      .option("delimited",",")
      .load("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\Hospital_inpatient.csv")
    /*  .withColumnRenamed("DRG Definition","DRG_Definition")
      .withColumnRenamed("Provider Id","Provider_Id")
      .withColumnRenamed("Provider Name","Provider_Name")
      .withColumnRenamed("Provider Street Address","Provider_Street_Address")
      .withColumnRenamed("Provider City","Provider_City")
      .withColumnRenamed("Provider State","Provider_State")
      .withColumnRenamed("Provider Zip Code","Provider_Zip_Code")
      .withColumnRenamed("Hospital Referral Region Description","Hospital_Referral_Region_Description")
      .withColumnRenamed("Total Discharges","Total_Discharges" )
      .withColumnRenamed("  Average Covered Charges ","Average_Covered_Charges")
      .withColumnRenamed("Average Total_Payments","Average_Total_Payments")
      .withColumnRenamed("Average Medicare Payments","Average_Medicare_Payments")*/
       //.show(1000,false)


    var new_df = data

    for(col <- new_df.columns) {
      new_df = new_df.withColumnRenamed(col,col.replaceAll("\\s","_"))
    }

  //  new_df.printSchema()
    new_df.createOrReplaceTempView("hospital_charges")
  /*  spark.sql("select Provider_State,avg(_Average_Covered_Charges_) as avg_chr from hospital_charges group by Provider_State")
      .show(false)

    new_df.groupBy("Provider_State").avg("_Average_Covered_Charges_").show()*/




    //find out the total num of discharge per state and for each diseases.

  //  new_df.show(20,false)

    spark.sql("select Provider_State,DRG_Definition,sum(_Total_Discharges_) as total from hospital_charges group by Provider_State,DRG_Definition")
      .show()

    /*df.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").sort(desc(sum("TotalDischarges").toString)).show
    df.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").orderBy(desc(sum("TotalDischarges").toString)).show*/

  }
}


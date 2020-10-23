package DataFrame_SparkSQL_part_2

import org.apache.spark.sql.SparkSession

object DataFrame_Tutorial extends App with Context {

  sc.setLogLevel("ERROR")

  val dfTags = spark.read.option("header", true).option("inferSchema", true)
    .csv("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame_SparkSQL_part_2\\question_tags_10k.csv")
    .toDF("id", "tags")

  // dfTags.select("id","tags").show(10)
  // dfTags.filter("tags=='php' ").show(10)

  //dfTags.filter("tags like 's%'").show(10)

  // dfTags.filter("tags like 's%'").filter("id == 25 or id ==108").show(10)

  // dfTags.filter("id in (25,108)").show(10)

  /*  println("Grouping the tag values")
  dfTags.groupBy("tags").count().show(20)*/

  /*  println("Grouping with filter")
  dfTags.groupBy("tags").count().filter("count > 5").show()*/

  /*  println("order by ")
  dfTags.groupBy("tags").count().filter("count > 5").orderBy("tags").show(50)*/

  val dfQuestionCSV = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame_SparkSQL_part_2\\questions_10k.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")


  val dfQuestions = dfQuestionCSV.select(
    dfQuestionCSV.col("id").cast("integer"),
    dfQuestionCSV.col("creation_date").cast("timestamp"),
    dfQuestionCSV.col("closed_date").cast("timestamp"),
    dfQuestionCSV.col("deletion_date").cast("date"),
    dfQuestionCSV.col("score").cast("integer"),
    dfQuestionCSV.col("owner_userid").cast("integer"),
    dfQuestionCSV.col("answer_count").cast("integer")
  )

  //dfQuestions.printSchema()

  //  dfQuestions.show(10)


  val dfQuestionsSubSet = dfQuestions.filter("score > 400 and score <410 ")

  /* dfQuestionsSubSet.show()

  dfQuestionsSubSet.join(dfTags,"id")
    .select("owner_userid","tags","creation_date","score").show()
*/


  //joins types
  /*dfQuestionsSubSet.join(dfTags,Seq("id"),"left_anti")
    .show(10)*/


}
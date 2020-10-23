package DataFrame_SparkSQL_part_2.SparkSQL

import DataFrame_SparkSQL_part_2.DataFrame_Tutorial.{sc, spark}

object Spark_sql_tut extends App with Context
{

  sc.setLogLevel("ERROR")

  val dfTags = spark.read.option("header", true).option("inferSchema", true)
    .csv("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame_SparkSQL_part_2\\question_tags_10k.csv")
    .toDF("id", "tags")

  dfTags.createOrReplaceTempView("so_tags")


  //  spark.sql("select id,tags from so_Tags limit 20").show()
 // spark.sql("select id,tags from so_tags where tags = 'php' limit 10").show()
//spark.sql("select count(1) as cnt from so_tags where tags ='php'" ).show()
//spark.sql("select * from so_tags where tags like 's%' limit 10").show()
//spark.sql("select id from so_tags where tags like 's%' and (id =25 or id =108) limit 10").show()
//spark.sql("select * from so_tags where id in (25,108)").show()
//spark.sql("select tags,count(*) as cnt from so_tags group by tags limit 10").show()
//spark.sql("select tags,count(*) as cnt from so_tags group by tags having cnt > 5  order by tags limit 10").show()

val dfQuestionCSV =
  spark.read.option("header",true)
  .option("inferSchema",true)
  .option("dateFormat","yyyy-MM-dd HH:mm:ss")
  .csv("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame_SparkSQL_part_2\\questions_10k.csv")
  .toDF("id",
  "creation_date",
  "closed_date",
  "deletion_date",
  "score",
  "owner_userid",
  "answer_count")


  val dfQuestions = dfQuestionCSV.select(dfQuestionCSV("id").cast("integer"),
    dfQuestionCSV("creation_date").cast("timestamp"),
    dfQuestionCSV("closed_date").cast("timestamp"),
    dfQuestionCSV("deletion_date").cast("timestamp"),
    dfQuestionCSV("score").cast("integer"),
    dfQuestionCSV("owner_userid").cast("integer"),
    dfQuestionCSV("answer_count").cast("integer"))



  //filter dataframe
  val dfQuestionsSubset =dfQuestions.filter("score >400 and score <410").toDF()

/*  dfQuestionsSubset.createOrReplaceTempView("so_questions")*/

//  spark.sql("select t.* from so_tags t  left anti join  so_questions q on (t.id = q.id) limit 10").show()

/*  def prefixStackoverflow(s:String) = s"so_$s"

  spark.udf.register("prefix_so",prefixStackoverflow _)

  spark.sql("select id, prefix_so(tags) from so_tags limit 10").show()*/

import org.apache.spark.sql.functions._
 // dfQuestions.select(avg("score")).show()

  //dfQuestions.select(max("score")).show()

 // dfQuestions.select(min("score")).show()

  //dfQuestions.select(mean("score")).show()

/*dfQuestions
    .filter("id > 400 and id <410")
    .filter("owner_userid is not null")
    .join(dfTags, dfTags("id").equalTo(dfQuestions("id")))
    .groupBy(dfQuestions("owner_userid"))
    .agg(avg("score"),max("answer_count"))*/

val tagDF = spark.read.option("multiLine",true)
  .option("inferSchema",true)
  .json("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\DataFrame_SparkSQL_part_2\\tags_sample.json")



/*
  val df = tagDF.select(explode(col("stackoverflow")) as "stackoverflow_tags")
    .select(col("stackoverflow_tags.tag.id") as "id",
    col("stackoverflow_tags.tag.name") as "name",
    col("stackoverflow_tags.tag.author") as "author",
    col("stackoverflow_tags.tag.frameworks.id") as "framework_id" ,
    col("stackoverflow_tags.tag.frameworks.name") as "framework_name")
*/

  //df.show(10,false)

/*  df.select("*")
    .where(array_contains(col("framework_name"),"Play Framework"))
    .show()*/

  val donuts = Seq(("plain donut",1.50),("vanilla donut",2.0),("glazed donut",2.50))

 /* val df =spark.createDataFrame(donuts).toDF("Donut Name","Price")

// val priceColumnExists = df.columns.contains("Price")

  val targets = Seq(("Plain Donut",Array(1.50,2.0)),("vanilla Donut",Array(2.0,2.50)),
    ("Strawberry Donut",Array(2.50,3.50)))

import spark.sqlContext.implicits._
  val df_1 = spark.createDataFrame(targets).toDF("name","prices")

  df_1.show()

val df2 = df_1.select(col("name"),
  col("prices")(0).as("low prices"),
  col("prices")(1).as("High prices"))

  df2.show()
*/

  val df = spark.createDataFrame(donuts).toDF("Donut_Name","Price")

  val df2 = df.withColumn("Tasty",lit(true))
    .withColumn("correlation",lit(1))
    .withColumn("Stock Min Max", typedLit(Seq(100,500)))

  df2.show()




}

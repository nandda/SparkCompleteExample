package Practice


import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/*object SparkContextOld extends App{

val conf = new SparkConf().setMaster("local[*]").setAppName("SparkContextOld")
val sparkContext = new SparkContext(conf)
sparkContext.setLogLevel("ERROR")

 println("First SparkContext")
 println("App Name: "+sparkContext.appName)
 println("MasterName: "+sparkContext.master)
 println("DeploymentMode:"+sparkContext.deployMode)
sparkContext.stop()

val spark = SparkSession.builder().master("local[*]").appName("SparkSessionNew")
                          .getOrCreate()
val sc = spark.sparkContext

  println("First SparkSession")
  println("App Name: "+sc.appName)
  println("MasterName: "+sc.master)
  println("DeploymentMode:"+sc.deployMode)
  sc.stop()
}*/


/*

object RDDParallelize {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDParallelize")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")


    val RDD_data = sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    val RDD_data_collect = RDD_data.collect()

    println("Get the number of Partitions :" +RDD_data.getNumPartitions)
    println("App_name :"+sparkContext.appName)
    println("Action: First Elements: " +RDD_data.first())
    println("RDD converted array of int")
    RDD_data_collect.foreach(x=>println(x))
  sparkContext.stop()

    val spark = SparkSession.builder().appName("RDDParallelizeSparkSession").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val RDD_data_2 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    val RDD_data_collect_2 = RDD_data_2.collect()

    println("Get the number of Partitions_2 :" +RDD_data_2.getNumPartitions)
    println("APPName: "+sc.appName)
    println("Action: First Elements_2: " +RDD_data_2.first())
    println("RDD converted array of int")
    RDD_data_collect_2.foreach(x=>println(x))
  }
}
*/

/*object ReadMultipleFiles {
  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("ReadMultipleFiles")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")

    println("Read all text files from Single Directory")
    val rdd = sparkContext.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Practice\\Test_files")
  //  rdd.foreach(x=>println(x))

    println("Read all text files from wilcard operator")
    val rdd_2 = sparkContext.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Practice\\Test_files\\text*.txt")
    rdd_2.foreach(x=>println(x))

    println("read all text files with file names")
    val rdd_3 = sparkContext.wholeTextFiles("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Practice\\Test_files\\text*.txt")
    rdd_3.foreach(x=>println(x))

  }*/

/*
object ReadMultipleCSVFiles {
  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ReadMultipleCSV").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")

    val rddFromFile = sparkContext.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Practice\\Test_files\\test01.csv")
    println(rddFromFile.getClass)

    val rdd = rddFromFile.map(f=> {
      f.split(",")
    })

  /*  println("Iterate RDD")
    rdd.foreach(f=>{
      println("Col1:"+f(0)+",Col2:"+f(1))
    })
    println(rdd)*/


    println("read csv files into RDD")
    val rdd4 = sparkContext.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Practice\\Test_files\\test01.csv")
   val rdd5= rdd4.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    rdd5.foreach(x=>println(x))

  }
*/
/*
object CreatingRDD {
  def main(args:Array[String]): Unit = {
    import org.apache.spark.sql

    /*val conf = new SparkConf().setMaster("local[*]").setAppName("CreatingRDD")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")*/

    val spark = SparkSession.builder().appName("CreatingRDD").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rdd =sc.parallelize(Seq(("Java",20000),("Python",100000),("Scala",3000)))
    rdd.foreach(x=> println(x))

    val rdd2 =sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Practice\\Test_files\\text02.txt")
    println("rdd partitions: "+rdd2.getNumPartitions)

   import spark.implicits._
    val rdd3 =sc.range(1,20).toDF().rdd

  }*/
/*

object CreateEmptyRDD extends App {

  val spark = SparkSession.builder().master("local[*]").appName("CreateEmptyRDD").getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val rdd = sc.emptyRDD
  val rddString = sc.emptyRDD[String]

  println(rdd)
  println(rddString)
  println("Num of Partitions :"+rdd.getNumPartitions)
  println("Num of Partitions :"+rddString.getNumPartitions)

  val rdd2 = sc.parallelize(Seq.empty[String])
  println(rdd2)
  println("Num of partitions "+rdd2.getNumPartitions)

  type dataType = (String,Int)
  val pairRDD = sc.emptyRDD[dataType]
  println("Num of partitions "+pairRDD.getNumPartitions)

  val pairRDD1 = sc.parallelize(Seq.empty[dataType])
  println("Num of partitions "+pairRDD1.getNumPartitions)
*/

/*
object WordCountExample {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Transformation").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val Rdd = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\test.txt")
    println("initial partition count:"+Rdd.getNumPartitions)

    val RepartitionRdd = Rdd.repartition(4)
    println("Repartition count:"+RepartitionRdd.getNumPartitions)

   // Rdd.collect().foreach(x=> println(x))

    //flatMap
    val flatMap_Rdd = Rdd.flatMap(x=> x.split(" "))
 //   flatMap_Rdd.foreach(x=>println(x))

    //Create a Tuple for adding each element with 1
    val Rdd3 = flatMap_Rdd.map(m=> (m,1))
  //  Rdd3.foreach(x=>println(x))

    //Filter transformation
    val rdd4 = Rdd3.filter(a=> a._1.startsWith("a"))
  //  rdd4.foreach(x=>println(x))

   //ReduceByKey transformation

    val rdd5 = rdd4.reduceByKey(_+_)
   // rdd5.foreach(x=>println(x))

    //swap word count and sortByKey transformation
    val rdd6= rdd5.map(a=>(a._1,a._2)).sortByKey()
    println("Final Result")
    //rdd6.foreach(x=>println(x))

    //println("Count :"+rdd6.count())

    val firstRec =rdd6.first()
    //println("First Record: "+firstRec._1+","+firstRec._2)

    val datMax = rdd6.max()
    //println("Max Record: "+datMax._1+","+datMax._2)

    //Action-reduce
    rdd6.foreach(x=>println(x))
    val totalWordCount =rdd6.reduce((a,b) => (a._1+b._1,a._2))
    println("dataReduce Record :"+totalWordCount)

  }

}
*/

/*
object Actions {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Actions").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val inputRDD = sc.parallelize(List(("A", 20),("B", 30),("C", 40),("B", 30),("B", 60),("Z",1)))
    val listRDD = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))

    def param0 = (accu:Int,v:Int) => accu + v
    def param1 = (accu1:Int,accu2:Int) => accu1 + accu2

    println("aggregate: "+listRDD.aggregate(0)(param0,param1))

    def param2 =(accu:Int,v:(String,Int)) => accu + v._2
    def param3 =(accu1:Int,accu2:Int) => accu1 + accu2

    println("aggregate:"+inputRDD.aggregate(0)(param2,param3))

    //treeaggregate
    def param4 = (accu:Int,v:Int) => accu + v
    def param5 =(accu1:Int,accu2:Int) => accu1 +accu2

    println("treeAggregate: "+listRDD.aggregate(0)(param4,param5))

    //fold
    println("fold: "+listRDD.fold(0){(accu,v) =>
      val sum = accu+v
  sum})

    println("fold :"+inputRDD.fold("Total",0) {(accu,v) =>
      val sum = accu._2+v._2
      ("Total",sum)
    })

    //reduce
    println("reduce: "+listRDD.reduce(_+_))

    //reduce
    println("reduce: "+inputRDD.reduce((a,b) => ("total",a._2+b._2)))

    println("treeReduce: "+listRDD.treeReduce(_+_))

    println("Count: "+listRDD.count())
    println(("Count Approx:"+listRDD.countApprox(1200)))

    println("Count Approx Distinct: "+listRDD.countApproxDistinct())


    println("CountByValue : "+listRDD.countByValue())
    println("CountByValue :" +inputRDD.countByValue())

    //take
    println(listRDD.take(4).mkString("|"))
    println(listRDD.takeOrdered(4).mkString(","))

  }
}*/

/*
object OperationsOnPairRDD {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("PairRDD").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val rdd = sc.parallelize(List(" Germany India","USA","USA India Russia","India Brazil Canada China"))

    val wordsRDD = rdd.flatMap(x=> x.split(" "))
    val pairRDD = wordsRDD.map(f=> (f,1))
    pairRDD.foreach(x=>println(x))

    println("Distinct==>")
    val pairRDD2 =pairRDD.distinct().foreach(x=>println(x))

  println("SortByKey===>")
    val pairRDD3 =pairRDD.sortByKey()

  //  pairRDD3.foreach(x=>println(x))

//reduceByKEy
    val wordCount = pairRDD3.reduceByKey((a,b)=> (a+b))
    wordCount.foreach(x=>println(x))


      //aggregateByKey
    def param0 = (accu:Int,V:Int) => accu + V
   def param1 =(accu1:Int,accu2:Int) => accu1 +accu2

    val wordCount2 = pairRDD.aggregateByKey(0)(param0,param1)
    wordCount2.foreach(x=>println(x))

    pairRDD.collectAsMap().foreach(x=>println(x))
  }
}*/
/*

object Shuffling {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Shuffling").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions",100)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import  spark.implicits._
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )

    val df =simpleData.toDF("employee_name","department","state","salary","age","bonus")

    val df2 = df.groupBy("state").count()
    println(df2.rdd.getNumPartitions)

  }
}*/
/*

object BroadCastVariable {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("BroadCastVariables").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries = Map(("USA","United States of America"),("IN","India"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val data = Seq(("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    )

    val rdd = spark.sparkContext.parallelize(data)

    val rdd2 = rdd.map(f=> {
      val country = f._3
      val state = f._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState  =broadcastStates.value.get(state).get
      (f._1,f._2,fullCountry,fullState)
    })

    rdd2.foreach(x=>println(x))
  }
}*/


/*
object BroadCastVariableDataFrame {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries = Map(("USA","United States of America"),("IN","India"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val data = Seq(("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    )

    val columns = Seq("FirstName","LastName","Country","State")
    import spark.implicits._
    val df = data.toDF(columns:_*)

    val df2 = df.map(row => {
      val country = row.getString(2)
      val state = row.getString(3)
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState =broadcastStates.value.get(state).get
      (row.getString(0),row.getString(1),fullCountry,fullState)
    }).toDF(columns:_*)

    df2.show(false)
  }
}*/
/*
object FileAlreadyExistProblem {

  import org.apache.spark.sql.{SaveMode, SparkSession}

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark SQL FileAlreadyExist error example")
      .getOrCreate()

    import sparkSession.implicits._

    val dataset = Seq(
      (1, "a"), (2, "b"), (3, "c"), (4, "d"), (0, ".")
    ).toDF("nr", "letter").repartition(3)

    val datasetWithDivisionResults = dataset.map(row => {
      try {
        30/row.getAs[Int]("nr")
      } catch {
        case e => {
          val previousFlag = FailedFlag.isFailed
          FailedFlag.isFailed = true
          if (!previousFlag) throw e else 0
        }
      }
    })

    datasetWithDivisionResults
      .write.mode(SaveMode.Overwrite)
      .json("test-file-already-exists")
  }

}

object FailedFlag {
  var isFailed = false
}*/

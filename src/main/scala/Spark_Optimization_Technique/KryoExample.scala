package Spark_Optimization_Technique

//https://stackoverflow.com/questions/42956220/submitting-pyspark-app-to-spark-on-yarn-in-cluster-mode
//https://blog.knoldus.com/kryo-serialization-in-spark/
//https://github.com/pinkusrg/spark-kryo-example/blob/master/src/main/scala/KyroExample.scala
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KryoExample extends App {

  case class Person(name:String,age:Int)

  val conf = new SparkConf()
    .setAppName("Kryo_serialization")
    .setMaster("local[*]")
    .set("spark.serializer","org.apache.spark.serializer.kryoSerializer")
    .set("setWarnUnregisteredClasses","true")
    .set("spark.kryo.registrationRequired","true")
    .registerKryoClasses(
      Array(classOf[Person])
    )

  //if we not registered the kryoClasses, then its take default serialization.
/*    .registerKryoClasses(
      Array(classOf[Person],classOf[Array[Person]],
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))*/

  val sparkContext = new SparkContext(conf)
      sparkContext.setLogLevel("ERROR")

  val personList = (1 to 99999).map(value => Person("P"+value,value)).toArray

  val rddPerson =sparkContext.parallelize(personList,5)
  val evenAgePerson =rddPerson.filter(_.age %2 == 0)

  evenAgePerson.persist(StorageLevel.MEMORY_ONLY_SER)

  evenAgePerson.take(50).foreach(person=> println(person.name,person.age))

}

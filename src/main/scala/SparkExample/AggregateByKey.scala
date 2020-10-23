package SparkExample

import org.apache.spark.sql.SparkSession

object AggregateByKey {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("AggregateByKey").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
     sc.setLogLevel("ERROR")
    val studentRDD = sc.parallelize(Array(
      ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
      ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
      ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
      ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
      ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
      ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)

/*    //Sequence Operation to find the maximum marks.
    def SeqOp(accum: Int, element: (String, Int)) = {
      if (accum > element._2) accum else element._2
    }

    //Combiner Operation to find the maximum marks:
    def ComOp(accum1: Int, accum2: Int) = {
      if (accum1 > accum2) accum1 else accum2
    }

    val zero_val = 0

    val output = studentRDD.map(x => (x._1, (x._2, x._3)))
      .aggregateByKey(0)(SeqOp, ComOp)
    output.foreach(f => println(f))*/

    //subject Name along with marks
/*    val zero_val = (" ",0)

    def SeqOp(accum:(String,Int),element:(String,Int)):(String,Int)= {
      if(accum._2 > element._2) accum else element
    }

    def ComOp(accum1:(String,Int),accum2:(String,Int)) = {
      if(accum1._2 >accum2._2) accum1 else accum2
    }

    val output = studentRDD.map(x=> (x._1,(x._2,x._3)))
      .aggregateByKey(zero_val)(SeqOp,ComOp)

    output.foreach(f=>println(f))*/

  //Percentage wise student marks

    val zero_val = (0,0)
    def SeqOp(accum:(Int,Int),element:(String,Int)) = {
      (accum._1+element._2,accum._2+1)
    }
    def ComOP(accum1:(Int,Int),accum2:(Int,Int)) = {
      (accum1._1+accum2._1,accum1._2+accum2._2)
    }

    val output = studentRDD.map(x=> (x._1,(x._2,x._3)))
      .aggregateByKey(zero_val)(SeqOp,ComOP)
        .map(x => (x._1,(x._2._1/x._2._2)))

   output.foreach(f=> println(f))
  }
}
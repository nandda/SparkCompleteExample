package DataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object DataFrameWithComplexDSL {

  case class Employee(firstName:String,lastName:String,email:String,salary:Int)
  case class Department(id:Int,name:String)
  case class DepartmentWithEmployees(department: Department,employee: Seq[Employee])
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ComplexDSL").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")



    val department1 = Department(123456, "Computer Science")
    val department2 = Department(789012, "Mechanical Engineering")
    val department3 = Department(345678, "Theater and Drama")
    val department4 = Department(901234, "Indoor Recreation")

    val employee1 = Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = Employee("matei", "", "no-reply@waterloo.edu", 140000)
    val employee4 = Employee("", "wendell", "no-reply@berkeley.edu", 160000)


    val departmentWithEmployees1 = DepartmentWithEmployees(department1,List(employee1,employee2))
    val departmentWithEmployees2 = DepartmentWithEmployees(department2,List(employee3, employee4))
    val departmentWithEmployees3 = DepartmentWithEmployees(department3,List(employee1, employee4))
    val departmentWithEmployees4 = DepartmentWithEmployees(department4,List(employee2, employee3))


    val data1 = List(departmentWithEmployees1,departmentWithEmployees2)
    val data2 = List(departmentWithEmployees3,departmentWithEmployees4)



    val df = spark.createDataFrame(data1)
    val df2 = spark.createDataFrame(data2)

    val finalDF = df.union(df2)
    finalDF.show(false)

  finalDF.select("department.*").printSchema()
finalDF.select(explode(col("employee"))).select("col.*").show(false)

  }
}

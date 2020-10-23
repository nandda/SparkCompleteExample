import java.io.Serializable


class Person extends Serializable {
  private var firstName :String= null
  private var lastName :String= null
  private var sex :String=null
  private var age :Long= 0L

  def getFirstName: String = firstName

  def setFirstName(firstName: String): String = {
    return firstName
  }

  def getLastName: String = lastName

  def setLastName(lastName: String): String = {
    return lastName
  }

  def getSex: String = sex

  def setSex(sex: String): String = {
    return sex
  }

  def getAge: Long = age

  def setAge(age: Long): Long = {
    return age
  }
}

/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes

object StringLengthUdf {
  def registerStringLengthUdf(spark: SparkSession): Unit = {
    spark.udf.register("stringLengthUdf", new UDF1[String, Long]() {
      override def call(str: String): Long = if (str != null && !str.isEmpty)  str.length
      else 0L
    }, DataTypes.LongType)
  }
}*/

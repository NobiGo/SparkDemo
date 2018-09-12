package DataSet

import org.apache.spark.sql.SparkSession
import util.PrintUtil

/**
  * @Author: duanxiong5
  * @Date: 2018/8/31 14:21
  */
object CreateDataset {

  /**
    * case class 在初始化对象时候不用new
    * case class 默认是可以序列化的
    * case class 构造函数的参数级别是public级别的可以直接进行访问
    * case class 支持模式匹配
    *
    * @param name
    * @param age
    */
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSessionFactory.createSession()
    import sparkSession.implicits._
    val caseClassDs = Seq(Person("Andy", 32)).toDS()
    caseClassDs.show()
    PrintUtil.print("=")
    val primiticeDS = Seq(1, 2, 3).toDS()
    primiticeDS.map(_ + 1).collect().foreach(print)

    /**
      * DataFrame 转化为 DataSet
      */
      println("DataFrame 转化为 DataSet")
    val path = "E:\\examples\\src\\main\\resources\\people.json"
    val peopleDS = sparkSession.read.json(path).as[Person]
    peopleDS.show
  }
}

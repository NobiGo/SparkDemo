package DataSet

import org.apache.spark.sql.DataFrame
import util.PrintUtil

/**
  * @Author: duanxiong5
  * @Date: 2018/8/31 11:47
  */

/**
  * DataFrames provide a domain-specific language for 结构化数据
  */
object CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSessionFactory.createSession()
    import sparkSession.implicits._
    /**
      * DataFrameReader 能够读取各种形式的文件（由sparkSesison.read返回）
      */
    val df: DataFrame = sparkSession.read.json("E:\\examples\\src\\main\\resources\\people.json")
    df.show
    PrintUtil.print("=")
    df.printSchema
    PrintUtil.print("*")
    df.select("name").show
    PrintUtil.print("+")
    //    df.filter($"age" > 21).show()
    df.select("name", "age").show
    df.groupBy("age").count().show
    df.select($"name", $"age" + 1).show()

    /**
      * Running SQL Queries Programmatically
      *
      * 临时View声明周期比较短
      * 如果session关闭，临时的View将消失
      */
    df.createOrReplaceTempView("people")
    val sqlDF: DataFrame = sparkSession.sql("SELECT * FROM people where age = 30")
    sqlDF.show()

    /**
      * 全局永久View
      */
    df.createGlobalTempView("people")
    sparkSession.sql("select * from people").show
    SparkSessionFactory.createSession().sql("select age from people").show
  }
}

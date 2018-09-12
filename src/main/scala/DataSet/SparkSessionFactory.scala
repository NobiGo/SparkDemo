package DataSet

import org.apache.spark.sql.SparkSession

/**
  * @Author: duanxiong5
  * @Date: 2018/8/31 11:26
  */

object SparkSessionFactory {
  var sparkSession: SparkSession = null

  def createSession(): SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession.builder().appName("Spark Sql Session").config("spark.master","local").getOrCreate
    }
    sparkSession
  }

  def close(): Unit = {
    sparkSession.stop
  }

}
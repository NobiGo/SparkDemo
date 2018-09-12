package RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: duanxiong5
  * @Date: 2018/8/31 11:26
  */
object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "hdfs://ds0:8020/var/log/SERVICE-HADOOP-6f3a74c66d6145a784b2551e98324a33/yarn/apps/duanxiong5/logs/application_1535366995908_0072/ds1_38235"
    val sparkConf: SparkConf = new SparkConf().setAppName("Simple App").setMaster("local")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    //2 minPartitions
    val logData = sparkContext.textFile(logFile, 2).cache()
    val numAs = logData.filter(lines => lines.contains("a")).count
    val numBs = logData.filter(lines => lines.contains("b")).count
    println("lines with a" + numAs)
    println("lines with b" + numBs)
    sparkContext.stop
  }
}

package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: duanxiong5
  * @Date: 2018/8/29 20:21
  */
object ExternalDatasets {
  /**
    * Spark也能由外部的数据集来新建分布式数据集
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("External Dataset").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    //返回把文件的行作为集合的分布式数据集
    val linesFile: RDD[String] = sparkContext.textFile("hdfs://ds0:8020/var/log/SERVICE-HADOOP-6f3a74c66d6145a784b2551e98324a33/yarn/apps/duanxiong5/logs/application_1535366995908_0072/ds1_38235");
    //    println(linesFile.count)
    //    println(linesFile.first)
    //获取文件中单词的总个数
    val wordsValue = linesFile.map(lines => lines.split(" ").length).reduce { (x, y) => (x + y) }
    println("单词数总共有" + wordsValue)
    //获取最长的句子的单词个数
    val wordsLinesMax = linesFile.map(lines => lines.split(" ").length).reduce((x, y) => if (x > y) x else y)
    println("最长句子的单词个数为" + wordsLinesMax)
    //获取最短的句子的单词个数
    val wordsLinesMin = linesFile.map(lines => lines.split(" ").length).reduce((x, y) => if (x > y) y else x)
    println("最短的句子的单词个数" + wordsLinesMin)
    //文件中总的行数
    val lines = linesFile.map(lines => 1).reduce((x, y) => x + y)
    println("文件中的总行数为" + lines)
    //文件中数据的总长度k
    val number = linesFile.map(lines => lines.length).reduce((x, y) => x + y)
    println("文件中数据的总长度" + number)
    //文件中每行数据出现的次数
    val lineNumber = linesFile.map(line => (line, 1)).reduceByKey((x, y) => x + y)
    println("文件中每行数据出现的次数为")
    lineNumber.take(100).foreach(println)

    /**
      * reduce 促使Spark将计算划分为不同的任务在分隔的集群上执行
      */
  }
}

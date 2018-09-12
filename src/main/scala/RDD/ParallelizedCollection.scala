package RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: duanxiong5
  * @Date: 2018/8/29 20:12
  */
object ParallelizedCollection {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local").
      setAppName("ParallelizedCollection")
    //sparkContext 负责与cluster建立连接
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val data = Array(1, 2, 3, 4, 5, 6)
    //Spark will run one task for each partition of the cluster.(一个partition一个task)
    val distData = sparkContext.parallelize(data)
    println(distData.first)
  }
}

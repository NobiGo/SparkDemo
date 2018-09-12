package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: duanxiong5
  * @Date: 2018/8/30 8:59
  */
object PassingFunction {
  //为Spark API 传递函数
  def func(value: Int): Int = {
    value + 1
  }

  def main(args: Array[String]): Unit = {
    //Array调用它的Apply方法新建一个数组
    val array: Array[Int] = Array(1, 2, 3, 4, 54, 56, 56);
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("Passing Function").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    //将Array转为一个分布式数据集
    val distData: RDD[Int] = sparkContext.parallelize(array)
    val newData = distData.map(func)
    //foreach方法不反悔值
    newData.foreach(println)
  }
}

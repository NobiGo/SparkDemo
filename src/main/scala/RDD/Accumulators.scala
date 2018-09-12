package RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: duanxiong5
  * @Date: 2018/8/30 16:01
  */
object Accumulators {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local").
      setAppName("Accumulators")
    //sparkContext 负责与cluster建立连接
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val accum = sparkContext.longAccumulator("My Accumulator")
    sparkContext.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x));
    println(accum)
    sparkContext.stop
  }

  /**
    * 累加器是Spark提供的一种分布式的变量机制，即分布式的改变然后再聚合这些改变。
    * 累加器的一个常见用途是在调试时对作业执行过程中的事件进行计数。
    *
    */
}

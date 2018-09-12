package RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: duanxiong5
  * @Date: 2018/8/30 15:33
  */
object BroadcastVariables {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local").
      setAppName("BroadcastVariables")
    //sparkContext 负责与cluster建立连接
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val broadcastVar = sparkContext.broadcast(Array(1, 2, 3));
    broadcastVar.value.foreach(println)
  }

  /**
    * 在广播变量创建后，在集群中所有函数将以变量V代表该广播变量，而且该变量v一次性分发到各节点上
    */
}

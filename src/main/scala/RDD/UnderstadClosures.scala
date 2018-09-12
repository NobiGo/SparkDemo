package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: duanxiong5
  * @Date: 2018/8/30 9:52
  */
object UnderstadClosures {
  def main(args: Array[String]): Unit = {
    //    var count = 0;
    val value: Array[Int] = Array(1, 2, 3, 4, 5, 6);
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("Understand Closures").
      setMaster("local")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val count = sparkContext.longAccumulator("My Accumulator")
    val distData: RDD[Int] = sparkContext.parallelize(value)
    distData.foreach(x => count.add(x))
    println(count)
    distData.foreach(println)
  }

  /**
    * 主要的挑战是，上面代码的执行结果是不确定的。在单一虚拟机的本地环境中，上面的代码会计算RDD所有变量的和并存储到counter变量中。
    * 这是因为RDD和变量counter存储在驱动节点的同一内存空间中。
    *
    * 然而，在集群模式中，上面的代码甚至都不能按预期工作。为了执行工作，Spark将RDD的操作划分为不同的任务，每个任务被一个执行体执行。
    * 在执行之前，Spark会计算闭包。由于闭包的存在，为了执行RDD上的计算，这些变量和方法必须对所有的执行体可见。这个闭包被序列化并发
    * 送到每个执行体。在本地模式中，只有一个执行体，所以所有的东西都共享同一个闭包。然而在其它的模式中，情况就不同了。执行体在不同
    * 的节点上执行，每个执行体都有一个闭包的拷贝。
    *
    * 这种情况下，包含变量的闭包的拷贝会发送到每个执行体，这样，在foreach里引用的counter就不是驱动节点中的counter了。在驱动节点中的
    * 内存里仍存在这一个counter,但是它对所有的执行体都是不可见的。执行体只会看到拷贝中的变量。这样，counter的最终结果就会是0,这里的
    * counter还是驱动节点上的内存空间中.因为所有的操作都是在序列化了的闭包上的变量上进行的。
    *
    */
}

package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @Author: duanxiong5
  * @Date: 2018/8/31 18:41
  */


object CreateStreamingContext {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark Streaming")
    /**
      * Create a StreamingContext by providing the configuration necessary for a new SparkContext.
      */
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(1))

    /**
      * 设置数据来源
      */
    val lines = streamingContext.socketTextStream("localhost", 999)
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)
    wordCount.print
    streamingContext.start
    streamingContext.awaitTermination
  }
}

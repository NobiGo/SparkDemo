/**
  * @author dx
  * @date 2018/9/11 下午10:04
  */
object SparkSessionFactory {
  val sparkSession: SparkSession = null;

  def createSparkSession(name: String): SparkSession = {
    if (sparkSession == null) {
      sparkSession = new SparkSession.builder().master("spark://Linux:7077")
        .appName(name)
        .getOrCreate()
    }
    sparkSession
  }

  def stop(): Unit = {
    sparkSession.stop
  }
}

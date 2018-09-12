package DataSet

import org.apache.spark.sql.SparkSession

/**
  * @Author: duanxiong5
  * @Date: 2018/8/31 16:02
  */
object DataSources {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSessionFactory.createSession()
    //    val userDF: DataFrame = sparkSession.read.load("E:\\examples\\src\\main\\resources\\users.parquet")
    //    userDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    /**
      * 手动指定文件格式
      */
    //    val peopleDF = sparkSession.read.format("json").load("E:\\examples\\src\\main\\resources\\people.json")
    //    peopleDF.select("name", "age").write.format("parquet").save("people.parquet")

    /**
      * 直接利用SQL读取文件中的内容
      *
      * select * from json(json根据后面文件的格式进行确定)
      */
    val sqlDF = sparkSession.sql("SELECT * FROM json.`E:\\examples\\src\\main\\resources\\people.json`").show
    
  }

}

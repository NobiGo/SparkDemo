package DataSet

import DataSet.CreateDataset.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @Author: duanxiong5
  * @Date: 2018/8/31 14:42
  */
object DataSetWithRDD {
  def main(args: Array[String]): Unit = {
//    funcRddToDataFrameReflection
    funcRddToDataFrameSchema
  }


  private def funcRddToDataFrameReflection = {
    val sparkSession = SparkSessionFactory.createSession
    import sparkSession.implicits._
    //读取文件成为一个RDD
    val fileRdd: RDD[String] = sparkSession.sparkContext.textFile("E:\\examples\\src\\main\\resources\\people.txt");
    val personDF: DataFrame = fileRdd.map(_.split(",")).map(attributes => Person(attributes(0).trim, attributes(1).trim.toInt)).toDF
    //将DF注册为一个视图
    personDF.createOrReplaceTempView("people")
    //在视图上进行操作
    val result: DataFrame = sparkSession.sql("select * from people")
    //    result.show
    /**
      * 按照索引序号去元素
      */
    result.map(teenager => "Name:" + teenager(0)).show
    result.map(teenager => "age:" + teenager(1)).show

    /**
      * 按照索引名取元素
      */
    result.map(teenager => "Name" + teenager.getAs[String]("name")).show
    /**
      *
      */
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    personDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  }

  private def funcRddToDataFrameSchema(): Unit = {
    /**
      * 1. 创建RDD
      * 2. 创建Schema 匹配RDD中的内容
      * 3. 使RDD应用Schema
      */
    val sparkSession: SparkSession = SparkSessionFactory.createSession()
    val fileRDD: RDD[String] = sparkSession.sparkContext.textFile("E:\\examples\\src\\main\\resources\\people.txt")
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD: RDD[Row] = fileRDD.map(line => line.split(",")).map(line => Row(line(0).trim, line(1).trim))
    val peopleDF = sparkSession.createDataFrame(rowRDD, schema)
    peopleDF.createOrReplaceTempView("people")
    sparkSession.sql("select * from people").show
  }
}

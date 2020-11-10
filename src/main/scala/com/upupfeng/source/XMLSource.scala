package com.upupfeng.source

import com.upupfeng.lib.spark.SparkConfig
import com.databricks.spark.xml._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * @Date: 2019/9/24 8:21
  */
object XMLSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getSpark("xml-source")
    val path = "file:///D:/datasets/news_tensite_xml.full/news_tensite_xml.smarty.dat"

    import spark.implicits._
    val df = spark
      .read
      .option("rowTag", "doc")
      .option("charset", "UTF-8")
      .xml(path)

    val resDf = df.selectExpr("content", "contenttitle as title", "url")
      .withColumn("id", monotonically_increasing_id() + 200)
      .withColumn("description", concat(substring($"content", 0, 10), lit("...")))
      .withColumn("source", lit("搜狗实验室"))
      .withColumn("type", lit(99))
      .withColumn("thumb", lit("http://spark.apache.org/docs/latest/img/spark-logo-hd.png"))
      .withColumn("created_at", current_timestamp())
      .withColumn("modified_at", current_timestamp())

    println(s"length: ${resDf.count()}")
    resDf
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .options(Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "f_information",
        "url" -> "jdbc:mysql://localhost:3306/feng",
        "user" -> "root",
        "password" -> "123456"
      ))
      .save()
    StructType

  }

  case class BaseInformation(id: Long, title: String, content: String, url: String)

}

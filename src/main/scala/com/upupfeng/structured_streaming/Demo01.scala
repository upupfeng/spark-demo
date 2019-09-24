package com.upupfeng.structured_streaming

import com.upupfeng.lib.spark.SparkConfig
import org.apache.log4j.LogManager

/**
  * @Date: 2019/8/15 8:18
  */
object Demo01 {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getSpark("struct streaming")
    spark.sparkContext.setLogLevel("warn")
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1234)
      .load()

    import spark.implicits._
    val words =lines.as[String].flatMap(_.split(" "))

    val wordCount = words.groupBy("value").count()

    val query = wordCount
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}











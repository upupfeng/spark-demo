package com.upupfeng.spark_streaming

import com.upupfeng.lib.spark.SparkConfig
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
/**
  * @Date: 2019/9/11 20:30
  */
object SparkStreamingDemo01 {

  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getSpark("streaming-demo")
    spark.sparkContext.setLogLevel("WARN")
    val scc = new StreamingContext(spark.sparkContext, Milliseconds(5000))

    val lines = scc.socketTextStream("localhost", 8888)
    val res = lines
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .print()


    scc.start()
    scc.awaitTermination()

  }

}

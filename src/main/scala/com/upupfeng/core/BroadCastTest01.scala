package com.upupfeng.core

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/17 18:05
  */
object BroadCastTest01 {

  def main(args: Array[String]): Unit = {

    val enumArr = Array(1, 2, 3)
    val spark = SparkConfig.getHiveSpark("broadcast-demo")
    val sc = spark.sparkContext
    // 在Driver端广播出去
    val enumBD = sc.broadcast(enumArr)

    // 在Executor端使用broadcast.value
    val rdd = sc
      .parallelize(1 to 20)
      .map(enumBD.value(0) * _)
      .foreach(println(_))

    sc.stop()
  }
}

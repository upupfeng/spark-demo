package com.upupfeng.core

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/19 10:32
  */
object SparkVmMasterDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getVMSpark("VM-Master")

    val df = spark.table("ods_bi_ai.sougou_search_log")
    println(df.count())
  }
}

package com.upupfeng.bloom_filter

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/18 8:45
  */
object SparkBloomFilter {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getHiveSpark("Spark-Bloom-Filter")

    val logDF = spark.table("ods_bi_ai.sougou_search_log")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    // 基于DF训练布隆过滤器
    val bf = logDF
      .stat
      .bloomFilter($"user_id", 10000, 0.001)

    println(bf.mightContain("2982199073774412"))
    println(bf.mightContain("29821990737744121231d"))

  }
}

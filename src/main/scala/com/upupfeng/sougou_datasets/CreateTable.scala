package com.upupfeng.sougou_datasets

import com.upupfeng.lib.spark.SparkConfig
import com.upupfeng.lib.util.Generator

/**
  * @Date: 2019/10/11 19:16
  */
object CreateTable {

  def main(args: Array[String]): Unit = {


    val spark = SparkConfig.getHiveSpark("bi_ai")

    // spark.sql("create database ods_bi_ai")
    // spark.sql("drop table ods_bi_ai.sougou_search_log")
    val txtRdd = spark
      .read
      .textFile("file:\\D:\\datasets\\SogouQ.mini\\SogouQ.sample")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 访问时间 用户ID [查询词] 该URL在返回结果中的排名 用户点击的顺序号 用户点击的URL
    val logDs = txtRdd
      .map(line => {
        val arr = line.split("\\s+")
        Log(arr(0), arr(1), arr(2), arr(3).toInt, arr(4).toInt, arr(5))
      })
      .withColumn("base_url", split($"url", "\\?")(0))
      .withColumn("sex", lit(Generator.rand1or2))
      .withColumn("ip", lit(Generator.randIp))
      .withColumn("is_hot", when($"res_rank".equalTo(1), 1).otherwise(2))
      .withColumn("view_time", to_timestamp(date_sub(current_timestamp(), Generator.ri)))

    //    logDs.selectExpr("base_url", "url")
    //        .show(10)

    logDs
      .write
      .format("parquet")
      .saveAsTable("ods_bi_ai.sougou_search_log")

    println("success~")
  }

  case class Log(
                  view_time: String,
                  user_id: String,
                  word: String,
                  res_rank: Int,
                  click_rank: Int,
                  url: String
                )

}

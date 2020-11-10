package com.upupfeng.sparksql

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/18 11:40
  */
object Repartition {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getHiveSpark("Repartition")
    val df = spark.table("ods_bi_ai.sougou_search_log")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    println(df.rdd.getNumPartitions)
    //    df
    //      .filter($"view_time" > 1)
    //      .coalesce(2)

    val df1 = df.repartition(5, $"user_id")

    println(df.rdd.getNumPartitions)
    println(df1.rdd.getNumPartitions)

  }

}

package com.upupfeng.sougou_datasets

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/14 21:34
  */
object TestNewUDF {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._
    val spark = SparkConfig.getHiveSpark("a")

    val sexUDF = udf((sex: Int) => if (sex == 1) "男" else "女")
    val hotUDF = udf((isHot: Int) => if(isHot == 1) "热门" else "普通")
    val aDF = spark
      .sql("select * from ods_bi_ai.sougou_search_log")
    aDF
      .withColumn("sex1", hotUDF(col("is_hot")))
      .show(20)
    aDF.select(col("*"), hotUDF(col("is_hot")).as("hot_name"))
      .show(20)

  }
}

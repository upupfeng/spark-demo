package com.upupfeng.lib.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Date: 2019/8/26 22:19
  */
object Execute {

  def sql(spark: SparkSession, sql :String): DataFrame ={
    println(sql)
    spark.sql(sql)
  }

}

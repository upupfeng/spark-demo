package com.upupfeng.sougou_datasets

import com.upupfeng.lib.spark.{Execute, SparkConfig}

/**
  * @Date: 2019/10/11 19:39
  */
object Test {

  def main(args: Array[String]): Unit = {
    val str = "12"
//    println(str.split("\\?")(0))
//

//    println(System.currentTimeMillis() % 2)

//    Execute.hiveQ(SparkConfig.getHiveSpark("a"), "select * from ods_bi_ai.sougou_search_log limit 20")
    Execute.hiveQ(SparkConfig.getHiveSpark("a"), "select count(*) from ods_bi_ai.sougou_search_log limit 20")




  }
}

package com.upupfeng.sougou_datasets

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/13 11:35
  */
object TestUDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getHiveSpark("udf")
//    spark.udf.register("sexUdf", sexUdf _)
//    spark.udf.register("sexUDF", (sex: Int) => if(sex == 1 ) "男" else "女")
    //    spark.sql(
    //      """
    //        |select sexUdf(sex) from ods_bi_ai.sougou_search_log limit 20
    //      """.stripMargin)

    import org.apache.spark.sql.functions._
    val baseDf = spark.sql("select * from ods_bi_ai.sougou_search_log")
    baseDf.selectExpr("sexUdf(sex)")
    baseDf
      .withColumn("sex_name", callUDF("sexUdf", col("sex")))
      .show(20)

  }


  def sexUdf(code: Int): String = {
    code match {
      case 1 => "男"
      case 2 => "女"
      case _ => "未知"
    }
  }

}

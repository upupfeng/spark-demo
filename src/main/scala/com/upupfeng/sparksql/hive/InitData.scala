package com.upupfeng.sparksql.hive

import com.upupfeng.lib.spark.{Execute, SparkConfig}

/**
  * @Date: 2019/8/26 22:11
  */
object InitData {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getSpark("1")

    Execute.sql(spark, "use spark_learning")
//    Execute.sql(spark,
//      """
//        |create table wf_account1(account_id int, modified_at timestamp, user_id int , platform_id int, name string, gender int, is_deleted int, created_time timestamp) stored as parquet
//      """.stripMargin)
    Execute.sql(spark,
      """
        |insert into wf_account1 values(1,"2019-01-01 00:00:00",1, 1, "demaxiya", 1, 2, "2019-01-01 00:00:00"),(2,"2019-01-01 00:00:00",1, 1, "demaxiya1", 1, 2, "2019-01-01 00:00:00")
      """.stripMargin)

    println("success")
  }
}

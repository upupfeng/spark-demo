package com.upupfeng.sparksql

import java.sql.Timestamp

import com.upupfeng.lib.spark.{Execute, SparkConfig}

/**
  * @Date: 2019/8/26 22:33
  */
object UserAccountDemo {

  case class WfUser(userId: Int, username: String, password: String,
                   phone: String, createdTime: Timestamp,
                    flag: Int, modifiedAt: Timestamp)

  case class WfAccount(accountId: Int, modifiedAt: Timestamp, userId: Int,
                       platformId: Int, name: String, gender: Int, isDeleted: Int,
                       createdTime: Timestamp)
  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getSpark("demo")
    val userSql = "select * from spark_learning.wf_user"
    val accountSql = "select * from spark_learning.wf_account"
    import spark.implicits._
    val userDs = Execute.sql(spark, userSql).as[WfUser]
    val accountDs = Execute.sql(spark, accountSql).as[WfAccount]

    import org.apache.spark.sql.functions._
    accountDs
      .joinWith(userDs, $"aaa" === $"b")
      .filter( f => (f._1.gender == 1 || f._2.flag == 2))
      .select($"username", $"bbb")

//    accountDs
//      .groupBy("platform_id")
//      .agg(expr("count(*) as cnt"), expr("sum(is_deleted) as id"))
//      .select("platform_id")
//      .where()

  }

}

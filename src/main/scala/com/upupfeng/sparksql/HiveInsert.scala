package com.upupfeng.sparksql

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/8/23 22:19
  */
object HiveInsert {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getSpark("insert")
    //    spark.sql("create database spark_learning")
    //    spark.sql("use spark_learning")
    //    spark.sql("insert into wf_user values(1,\"mwf\", \"123456\", \"15513288888\", \"2019-01-01 00:00:00\", 1, \"2019-07-01 00:00:00\"),(2,\"wb\", \"123456\", \"15513288888\", \"2019-01-01 00:00:00\", 1, \"2019-07-01 00:00:00\"),(3,\"zs\", \"123456\", \"15513288888\", \"2019-01-01 00:00:00\", 1, \"2019-07-01 00:00:00\")")

//
//    import spark.implicits._
//    val seq = Seq(People(1, "zs"), People(2, "ls"))
//    val ds = spark.createDataset(seq)
//    ds.write
//      .saveAsTable("spark_learning.people3")
//        spark.sql("select * from spark_learning.people").show()
        spark.sql("drop table spark_learning.people").show()

  }

  case class People(id: Int, name: String)

}

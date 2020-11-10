package com.upupfeng.sparksql.test

import java.lang

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/10 19:54
  */
object HiveQuery {

  case class A(id: Int, name: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getHiveSpark("test")
//    spark.sql("create database aaaa")

//    spark.sql("use aaaa")
//    val seq = Seq(A(1, "zs"), A(2, "ls"))
//    import spark.implicits._
//    val ds = spark.createDataset(seq)
//    ds.write.saveAsTable("test.b")


//    spark.sql("use spark_learning")
//    val df = spark.sql("show tables");
//    df.show()

    val df = spark.sql("select * from test.a")
    df.show()

    import org.apache.spark.sql.functions._
    df.withColumn("time", current_timestamp())
      .write
      .saveAsTable("test.a_with_time")

  }
}

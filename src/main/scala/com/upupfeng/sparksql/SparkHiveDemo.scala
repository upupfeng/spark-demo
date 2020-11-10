package com.upupfeng.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @Date: 2019/8/22 21:39
  */
object SparkHiveDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkHiveDemo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val txtRdd = spark.sparkContext.textFile("D:\\big_data\\dataset\\item_desc_dataset")
    import spark.implicits._
    val itemDs = txtRdd
    .map(f => f.split("\\t"))
    .map(arr => Item(arr(0), arr(1)))
    .toDS()

    spark.sql("use spark_learning")
    itemDs
      .write
      .saveAsTable("item_desc")
  }

  case class Item(name:String, description: String)
}

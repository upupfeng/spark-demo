package com.upupfeng.sparksql

import org.apache.spark.sql.SparkSession

/**
  * @Date: 2019/8/23 20:29
  */
object SparkHiveMetaStore {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("hiveMetaStore")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://192.168.168.101:9083")
      .getOrCreate()

    val df = spark.sql("show databases")
    df.show()
  }

}

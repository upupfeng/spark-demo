package com.upupfeng.lib.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Date: 2019/8/15 8:13
  */
object SparkConfig {

  def getSpark(name: String): SparkSession ={
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(name)
      .getOrCreate()
    spark
  }


  def getHiveSpark(name: String): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(name)
            .enableHiveSupport()
            .config("spark.sql.warehouse.dir", "hdfs://hadoop01:9000/user/hive/warehouse")
            .config("hive.metastore.uris", "thrift://192.168.168.101:9083")
      .getOrCreate()


    spark.sparkContext.hadoopConfiguration.addResource(new org.apache.hadoop.fs.Path(getClass.getResource("/hadoop-conf/core-site.xml").getPath))
    spark.sparkContext.hadoopConfiguration.addResource(new org.apache.hadoop.fs.Path(getClass.getResource("/hadoop-conf/hdfs-site.xml").getPath))
    spark.sparkContext.hadoopConfiguration.addResource(new org.apache.hadoop.fs.Path(getClass.getResource("/hadoop-conf/yarn-site.xml").getPath))


    spark
  }


  def getVMSpark(name: String): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .master("spark://hadoop01:7077")
      .appName(name)
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop01:9000/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://192.168.168.101:9083")
      .getOrCreate()

    spark
  }

}

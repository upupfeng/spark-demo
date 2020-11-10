package com.upupfeng

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Date: 2019/4/26 18:43
  */
object SparkDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("WordCount")
    val sc = new SparkContext(conf)

    val txt = sc.textFile("hdfs://hadoop01:9000/demo/words.txt")

    val res = txt.flatMap( line => line.split(" ") ).map( x => (x,1) ).reduceByKey( _+_ )

    res.foreach( f => {
      println(s"${f._1}:${f._2}")
    })

  }
}

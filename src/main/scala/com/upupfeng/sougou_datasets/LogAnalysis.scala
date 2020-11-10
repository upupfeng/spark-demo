package com.upupfeng.sougou_datasets

import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/10 23:08
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getSpark("sougou-log-analysis")

    val txtDs = spark
      .read
      .textFile("D:\\datasets\\SogouQ.mini\\SogouQ.sample")

    val count = txtDs.count()
    println(count)

    import spark.implicits._
    import org.apache.spark.sql.functions._
    txtDs
      .map(line => {
        val tmpArr = line.split("\\s+")
        Log(tmpArr(0), tmpArr(1), tmpArr(2), tmpArr(3).toInt, tmpArr(4).toInt, tmpArr(5))
      })
      .map(log => (log.word, 1))
      .rdd
      .reduceByKey(_ + _)
      .sortBy(f => f._2, false)
      .foreach(println(_))

    //
//      .reduceByKey("")
//      .red
//      .show(10)

  }

  case class Log(
                view_time: String,
                user_id: String,
                word: String,
                res_rank: Int,
                click_rank: Int,
                url: String
                )

}

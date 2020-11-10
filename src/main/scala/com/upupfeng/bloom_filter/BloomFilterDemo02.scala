package com.upupfeng.bloom_filter

import com.upupfeng.lib.spark.SparkConfig
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.util.sketch.BloomFilter

/**
  * 通过布隆过滤器过滤一个df在另一个df中出现/没出现的元素
  * @Date: 2019/10/18 10:03
  */
object BloomFilterDemo02 {

  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getSpark("bloom")
    val sc = spark.sparkContext

    import spark.implicits._
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val df1 = rdd1.toDF("c1")

    val rdd2 = sc.parallelize(Seq(1, 2, 3))
    val df2 = rdd2.toDF("c1")

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val itemNum = 1000
    val rpp = 0.005
    val bf1 = df1.stat.bloomFilter($"c1", itemNum, rpp)
    val bf2 = df2.stat.bloomFilter($"c1", itemNum, rpp)
    df1.show()
    df2.show()

    def contains(bf: BloomFilter) = udf((v: Int) => {
      bf.mightContain(v)
    })

    df1
      .where(!contains(bf2)($"c1"))
      .show()

  }

}


package com.upupfeng.core

import com.upupfeng.lib.spark.SparkConfig
/**
  * @Date: 2019/10/17 18:57
  */
object AccumulatorDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getSpark("Accumulator-Demo")
    val sc = spark.sparkContext
    // 在sc内部会注册累加器
    // 使用不当会导致不触发累加或重复触发累加
    val record_num = sc.longAccumulator("record_num")

    val rdd = sc.parallelize(Array(1, 2, 3))
    // 只写转换函数的话不会触发累加
    val newRdd = rdd.map(v => {
      record_num.add(1L)
      v * 2
    })

    // 调用多次行为操作会导致重复累加
    newRdd.count()
    newRdd.foreach(println(_))

    // 正确姿势
    val record_num2 = sc.longAccumulator("record_num2")
    rdd.foreach(v => {
      record_num2.add(1L)
      v * 2
    })
    println(record_num.value)
    println(record_num2.value)
  }
}

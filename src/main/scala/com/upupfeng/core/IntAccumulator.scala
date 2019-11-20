package com.upupfeng.core

import com.upupfeng.lib.spark.SparkConfig
import org.apache.spark.util.AccumulatorV2

/**
  * @Date: 2019/10/17 20:13
  */
class IntAccumulator extends AccumulatorV2[String, Int]{

  var count = 0
  // 返回此累加器是否是0值
  override def isZero: Boolean = count == 0
  // 复制一个累加器对象
  override def copy(): IntAccumulator = {
    val newIntAccumulator = new IntAccumulator
    newIntAccumulator.count = this.count
    newIntAccumulator
  }
  // 合并数据
  override def merge(other: AccumulatorV2[String, Int]): Unit = {
    other match {
      case o: IntAccumulator => this.count += o.count
      case _ => throw new Exception()
    }
  }
  // 获得累加器的值
  override def value: Int = count
  // 操作数据累加
  override def add(v: String): Unit = count += 1
  // 重置累加器中的数据
  override def reset(): Unit = count = 0
}
object IntAccumulator {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getSpark("Define-Accumulate-Demo")
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Array("1", "2", "3"))
    val count = new IntAccumulator
    // 自定义累加器在使用前必须先注册，否则会报序列化的错
    sc.register(count, "str_num")
    rdd.foreach(f => count.add(f))
    println(count)
  }
}







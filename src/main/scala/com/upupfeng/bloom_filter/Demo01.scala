package com.upupfeng.bloom_filter

import java.util

/**
  * BitSet
  * @Date: 2019/10/18 8:09
  */
object Demo01 {

  def main(args: Array[String]): Unit = {
    val bs = new util.BitSet()
    bs.set(1)

//    println(bs.get(0))
    println(bs.size())


  }
}

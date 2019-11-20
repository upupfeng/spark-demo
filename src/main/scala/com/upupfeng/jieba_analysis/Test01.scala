package com.upupfeng.jieba_analysis

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode

/**
  * @Date: 2019/10/17 8:02
  */
object Test01 {

  def main(args: Array[String]): Unit = {

    val sengenter = new JiebaSegmenter()
    val str = "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作"
    val list = sengenter.process(str, SegMode.INDEX)
    val it = list.iterator()
    var res = new StringBuffer
    while (it.hasNext) {
      res.append(", " + it.next().word)
    }

    val result = res.toString.substring(2)
    println(result)
  }
}

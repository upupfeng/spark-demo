package com.upupfeng.sougou_datasets

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.upupfeng.lib.spark.SparkConfig

/**
  * @Date: 2019/10/17 8:43
  */
object JiebaAnalysisTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkConfig.getHiveSpark("Jieba-Analysis")


    val baseDF = spark.table("ods_bi_ai.sougou_search_log")

    val jiebaUdf: String => String = (source: String) => {
      val segmenter = new JiebaSegmenter()
      val it = segmenter.process(source, SegMode.SEARCH).iterator()
      val sb = new StringBuffer
      while (it.hasNext) {
        sb.append(s", ${it.next.word}")
      }
      sb.toString.substring(2)
    }

    import org.apache.spark.sql.functions._
    val jieba = udf(jiebaUdf)
    val replace = udf(replaceUdf _)

    baseDF.show(20)

    import spark.implicits._
    baseDF
      .select($"*", jieba(replace($"word")).as("jieba_word"))
      .show(20)

  }

  def replaceUdf(word: String): String = {
    word.replaceAll("\\[|\\]", "")
  }


}

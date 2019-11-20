package com.upupfeng.sougou_datasets

import com.upupfeng.lib.spark.SparkConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

/**
  * @Date: 2019/10/15 8:18
  */
object CommonUADF {

  /**
    * UDAF实现count
    * 根据用户id分组计数
    */
  class VisitsUDAF extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = new StructType().add("userId", StringType)

    override def bufferSchema: StructType = new StructType().add("visits", IntegerType)

    override def dataType: DataType = IntegerType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getInt(0) + 1)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getInt(0)
    }

  }


  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getHiveSpark("a")
    spark.udf.register("acount", new VisitsUDAF)
    // 10002
//    spark.sql("select user_id, acount(user_id) from ods_bi_ai.sougou_search_log group by user_id")
//      .show()
    //
//    spark.sql("select count(user_id) from ods_bi_ai.sougou_search_log")
//      .show()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val logDF = spark.sql("select * from ods_bi_ai.sougou_search_log")

    logDF
      .groupBy(col("word"))
      .agg(new VisitsUDAF()(col("user_id")).as("uv"))
      .sort($"uv".desc)
      .show()

  }




}






package com.upupfeng.sparksql

import com.upupfeng.lib.spark.SparkConfig
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

/**
  * @Date: 2019/8/20 20:51
  */
object JsonReadWrite {

  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.getSpark("json-read-write")

    val structType = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("age", IntegerType)

    //以下是测试用的json
    //{"id": 1, "name": "mwf", "age": 23}
    //{"id": 2, "name": "zqr", "age": 23}

    //读json文件
    val jsonDF = spark
      .read
      .schema(structType)
      .json("D:\\learning_spark\\spark_demo\\src\\main\\resources\\read_json.jsonl")
    jsonDF.printSchema()
    jsonDF.show()

    //读json字符串
    val seq = Seq("{\"id\": 1, \"name\": \"mwf\", \"age\": 23}", "{\"id\": 2, \"name\": \"zqr\", \"age\": 23}")
    val jsonStringRdd = spark
      .sparkContext
      .parallelize(seq)
    import spark.implicits._
    // 也可以直接创建DS spark.createDataset(seq)
    val jsonDS= jsonStringRdd.toDS()
    val jsonDF2 = spark
      .read
      .json(jsonDS)
    jsonDF2.show()





  }
}

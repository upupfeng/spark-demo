package com.upupfeng.rdd

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Date: 2019/7/20 14:33
  */
/*
object RddDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val spark = new SparkSession(conf)


    case class People(id: Int, name: String)

//    val ps = Seq(People(1, "mwf"), People(2, "zqr"))
//    //peopleRDD的类型为RDD[Person]
//    val peopleRdd = spark.sparkContext.parallelize(ps)
//    peopleRdd.foreach( f => {
//      //f类型为People
//      println(s"${f.id} -- ${f.name}")
//    })
//
//    import spark.implicits._
//    //peopleDF的类型为DataFrame
//    val peopleDF = peopleRdd.toDF()
//    peopleDF.foreach( f => {
//      //f类型为Row
//
//      println(s"${f.getAs("id")} -- ${f.getAs("name")}")
//    })




    // 通过集合创建rdd
    // 调用parallelize会把集合中的数据拷贝到集群中，形成分布式的数据集合(RDD)
    val arr = Array("1,mwf","2,zqr")
    val rdd = spark.sparkContext.parallelize(arr)
    // 可以指定将集合切为几个分区
    val rdd1 = spark.sparkContext.parallelize(arr, 2)
    rdd.foreach(println(_))
    rdd1.foreach(println(_))

    //通过文本创建RDD
    val textRdd = spark.sparkContext.textFile("hdfs://hadoop01:9000/demo/words.txt")
    textRdd.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_+_)


    // 通过RDD[Row]和schema(StructType)创建dataframe
    // 创建schema(StructType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
    val schema1 = StructType(
      Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, false)
      ))
    val rowRdd = rdd.map(_.split(","))
        .map( x => Row(x(0).toInt, x(1)))
    spark.createDataFrame(rowRdd, schema)

    //通过隐式转换的方式创建dataframe
    //会隐式转换为Row类型
    import spark.implicits._
    val df = rdd.map(_.split(","))
      .map( x => People(x(0).toInt, x(1)))
      .toDF()
    df.show

    //从DF中取值
    df.map( row => {
      //通过下标取
      row.get(0)
      //通过字段名取
      row.getAs("id")
    })



    //DataFrame转为DataSet
    val ds = df.as[People]


    val seq = Seq(People(1, "mwf"))

    val peopleDF = seq.toDF()
    peopleDF.show()
    peopleDF.map( x => {
      x.getAs("id")
    })
    val peopleDs = seq.toDS()
    peopleDs.show()
    peopleDs.map( x => {
      x.id
    })

    val peoDs = spark.createDataset(seq)

    peoDs.rdd
    peopleDF.rdd


  }

}

*/

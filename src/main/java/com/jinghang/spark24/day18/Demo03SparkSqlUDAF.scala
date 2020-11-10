package com.jinghang.spark24.day18

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Create By 木木 On 2020.11.05 17:14 星期四
 * Desc:
 */
object Demo03SparkSqlUDAF
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate() //获取或者创建

    val df: DataFrame = spark.read.json("E://file/spark/people.json")

    //注册临时表
    df.createOrReplaceTempView("k")

    var sql = "select * from k"

    //求出名字的长度
    sql = "select name,length(name) from k"

    sql = "select name,avg(age) from k group by name"

    //自定义聚合函数
    sql = "select name,ageAvg(age) from k group by name"
    spark.udf.register("ageAvg", new AgeAvg())

    spark.sql(sql).show()

    spark.stop()
    sc.stop()
  }
}

class AgeAvg() extends UserDefinedAggregateFunction
{

  //输入数据类型
  override def inputSchema: StructType = StructType
  {
    Array(
      StructField("sum", LongType, true),
      StructField("count", IntegerType, true)
    )
  }

  //中间结果缓存的类型
  override def bufferSchema: StructType = StructType
  {
    Array(
      StructField("sum", LongType, true),
      StructField("count", IntegerType, true)
    )
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit =
  {
    buffer(0) = 0L
    buffer(1) = 0
  }

  //分区内的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
  {
    buffer(0) = buffer.getAs[Long](0) + input.getAs[Long](0) //age  相加
    buffer(1) = buffer.getAs[Int](1) + 1 //次数
  }

  //每个分区做合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
  {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Int](1) + buffer2.getAs[Int](1)
  }

  //输出结果
  override def evaluate(buffer: Row): Any =
  {
    buffer.getAs[Long](0).toDouble / buffer.getAs[Int](1)
  }
}

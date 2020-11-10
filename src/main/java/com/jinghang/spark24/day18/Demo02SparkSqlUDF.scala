package com.jinghang.spark24.day18

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Create By 木木 On 2020.11.05 16:59 星期四
 * Desc:
 */
object Demo02SparkSqlUDF
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
      .getOrCreate()

    val df: DataFrame = spark.read.json("E://file/spark/people.json")

    //注册成临时表
    df.createOrReplaceTempView("k")

    var sql = "select * from k"

    //求出每个名字的长度
    sql = "select name,length(name) from k"

    //自定义函数
    spark.udf.register("nameLength",(str:String) =>str.length)

    sql = "select name,nameLength(name) from k"

    spark.sql(sql).show()

    spark.stop()
    sc.stop()
  }
}

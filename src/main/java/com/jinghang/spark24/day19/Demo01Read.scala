package com.jinghang.spark24.day19

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo01Read {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    //读取数据源
//    spark.read.json(".json")
//    spark.read.textFile("")
//    spark.read.csv("")
//    spark.read.jdbc("","",properties)
//    spark.read.parquet("")
//    spark.read.orc("")

    //默认是 parquet 格式
//    val df: DataFrame = spark.read.load("E://file/spark/users.parquet")
//    df.show()

//    spark.read.format("json").load("")
//    spark.read.format("csv").load("")
//    spark.read.format("textfile").load("")
//    spark.read.format("orc").load("")
//    spark.read.format("parquet").load("")

    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark?serverTimezone=UTC")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "root")
      .load()

    df.show

    spark.stop()
    sc.stop()
  }
}

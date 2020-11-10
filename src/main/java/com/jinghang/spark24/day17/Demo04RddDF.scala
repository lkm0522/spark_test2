package com.jinghang.spark24.day17

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo04RddDF {

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

    val lineRdd = sc.textFile("E://file/spark/student01.txt")

    val rowRdd = lineRdd.map(line => {
      //class01 tom 100
      val splited = line.split(" ")
      Row(splited(0), splited(1), splited(2).toInt)
    })

    val schema = StructType(Array(
      StructField("classess",StringType,true),
      StructField("name",StringType,true),
        StructField("score",IntegerType,true)
    ))

    val df: DataFrame = spark.createDataFrame(rowRdd, schema)

    df.show()

    spark.stop()
    sc.stop()
  }
}

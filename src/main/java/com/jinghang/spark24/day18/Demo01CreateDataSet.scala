package com.jinghang.spark24.day18

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Create By 木木 On 2020.11.05 16:15 星期四
 * Desc:
 */
object Demo01CreateDataSet
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").enableHiveSupport().getOrCreate()

    import spark.implicits._
    val ds: Dataset[Person] = Seq(Person("小李", 56), Person("小明", 18)).toDS()

    ds.printSchema()

    val rdd: RDD[Person] = ds.rdd
    val javaRDD: JavaRDD[Person] = ds.toJavaRDD

    //DS转换成 DF
    val df: DataFrame = ds.toDF()

    //DF转换为 DS
    val ds01: Dataset[Person] = df.as[Person]

    //DF转换成RDD
    val rdd1: RDD[Row] = df.rdd

    //DF每一行都是ROW
    df.foreach(
      row =>
      {
        println(row.get(0) + "   " + row.get(1))
        //        row.getAs[String](0)
      }
    )

    spark.stop()
    sc.stop()
  }
}

case class Person(name: String, age: Int)
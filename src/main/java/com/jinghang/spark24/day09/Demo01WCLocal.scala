package com.jinghang.spark24.day09

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo01WCLocal {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    //创建sparkConf 对象   本地使用2core 来模拟计算
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    //创建sparkcontext 对象
    val sc = new SparkContext(conf)

    //读取数据源
    sc.textFile("E://file/spark/spark.txt")
        .flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .foreach(println(_))


//    val value: RDD[String] = sc.textFile("")
//    val value1: RDD[String] = value.flatMap(_.split(" "))
//    val value2: RDD[(String, Int)] = value1.map((_, 1))

    sc.stop()
  }
}

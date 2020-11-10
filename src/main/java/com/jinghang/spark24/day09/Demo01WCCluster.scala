package com.jinghang.spark24.day09

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo01WCCluster {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    //创建sparkConf 对象
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)

    //创建sparkcontext 对象
    val sc = new SparkContext(conf)

    //读取数据源   数据源以参数的形式来传递
    sc.textFile(args(0))
        .flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .saveAsTextFile(args(1))

    sc.stop()
  }
}

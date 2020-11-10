package com.jinghang.spark24.day15

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Create By 木木 On 2020.11.04 11:02 星期三
 * Desc:
 */
object Demo01StreamingWC {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //指定batch时间为5秒
    val ssc = new StreamingContext(sc, Seconds(5))

    //从socket获取数据源
    val dStream = ssc.socketTextStream("node7-1", 9999)

    dStream.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()    //保证程序一直运行，除非手动停止
  }
}

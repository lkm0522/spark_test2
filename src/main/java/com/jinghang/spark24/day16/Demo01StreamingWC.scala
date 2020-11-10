package com.jinghang.spark24.day16

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo01StreamingWC {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //指定batch 时间 5s
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    //从socket 获取数据源
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("node7-1", 9999)

    dStream.flatMap(_.split(","))
        .map((_,1))
        .reduceByKey(_+_)  //无状态
        .print()

    ssc.start()
    ssc.awaitTermination() //保证应用程序一直运行，除非手动停止
  }
}

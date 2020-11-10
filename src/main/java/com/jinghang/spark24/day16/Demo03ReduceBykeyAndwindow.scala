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
object Demo03ReduceBykeyAndwindow {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //指定batch 时间 5s
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    //The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint().
    ssc.checkpoint("E://file/spark/out/1103")

    //从socket 获取数据源
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("node7-1", 9999)

    dStream.flatMap(_.split(","))
        .map((_,1))
  /*
        windowDuration: Duration,  窗口的长度
        slideDuration: Duration, 滑动间隔
   */
      .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(5 ))

      .checkpoint(Seconds(50)) //至少是10s 以上，一般是batch的5-10倍
        .print()
//        .saveAsTextFiles("")
//        .foreachRDD(rdd=>{
//          rdd.foreach(println(_))
//        })


    ssc.start()
    ssc.awaitTermination() //保证应用程序一直运行，除非手动停止
  }
}

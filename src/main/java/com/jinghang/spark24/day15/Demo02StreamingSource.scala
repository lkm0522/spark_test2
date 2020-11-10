package com.jinghang.spark24.day15

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Create By 木木 On 2020.11.04 11:31 星期三
 * Desc:
 */
object Demo02StreamingSource
{
  def main(args: Array[String]): Unit =
  {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    //读取数据源
    //    ssc.socketTextStream("jinghang01",9999)

    //文件流
    //    ssc.textFileStream("E://")

    //队列
    //创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    ssc.queueStream(rddQueue)
      .print()

    ssc.start()

    //循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5)
    {
      //ssc.sparkContext
      rddQueue += sc.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}

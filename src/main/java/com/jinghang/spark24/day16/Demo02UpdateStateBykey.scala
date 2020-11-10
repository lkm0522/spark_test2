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
object Demo02UpdateStateBykey {

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
      //带状态的计算
//        .updateStateByKey( (values:Seq[Int],option:Option[Int]) =>{
//          //values  当前batch 所有值的集合
//          //option 前面batch 累计的结果状态
//          var sum = values.sum
//          val pre = option.getOrElse(0)
//          Option(sum+pre)  //
//        })

      .updateStateByKey( (values:Seq[Int],option:Option[(Int,Int)]) =>{
        //values  当前batch 所有值的集合
        //option 前面batch 累计的结果状态
        var sum = values.sum
        var count = values.length

        val tuple: (Int, Int) = option.getOrElse((0, 0))

        sum += tuple._1
        count += tuple._2

        Option((sum,count))
      })
      .map(tuple => (tuple._1, tuple._2._1.toDouble/tuple._2._2))

      .checkpoint(Seconds(50)) //至少是10s 以上，一般是batch的5-10倍
        .print()

    ssc.start()
    ssc.awaitTermination() //保证应用程序一直运行，除非手动停止
  }
}

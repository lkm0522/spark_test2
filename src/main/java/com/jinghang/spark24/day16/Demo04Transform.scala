package com.jinghang.spark24.day16

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo04Transform {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val list = Array(("tom",true), ("abc",true))
    val blackRdd = sc.parallelize(list)

    //指定batch 时间 5s
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    val dstream = ssc.socketTextStream("node7-1", 9999)

    //实时统计广告点击次数  通过过滤黑名单
    val pairDstream = dstream.flatMap(_.split(" "))
        .map((_,1))

    pairDstream.transform(userRdd =>{
          val joinedRdd: RDD[(String, (Int, Option[Boolean]))] = userRdd.leftOuterJoin(blackRdd)

          val filterRdd = joinedRdd.filter(tuple=>{
            val user = tuple._1
            val orElse = tuple._2._2.getOrElse(false)
            if(!orElse){
              true  //不是黑名单的留下了
            }else{
              false
            }
          })
          filterRdd.map(tuple => (tuple._1,tuple._2._1))
        })
        .reduceByKey(_+_)
      .print()
//      .foreachRDD(rdd=>{
//        rdd.foreachPartition()
//      })

//    val value: DStream[(String, (Int, Int))] = pairDstream.join(pairDstream)

    ssc.start()
    ssc.awaitTermination()

//    ssc.stop(false)  //停止ssc对象同时 是否要停掉 sc 对象

  }
}

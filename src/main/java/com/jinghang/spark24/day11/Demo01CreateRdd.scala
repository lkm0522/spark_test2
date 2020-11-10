package com.jinghang.spark24.day11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/28
 * desc:
 */
object Demo01CreateRdd {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //1.从内部集合
    val arr = Array(1, 2, 3, 4)
    val numRdd: RDD[Int] = sc.parallelize(arr,4)  //使用默认分区格式  也可以指定
    val numRdd01: RDD[Int] = sc.makeRDD(arr)

    //kv-Rdd
    val pairRdd: RDD[(Int, Int)] = numRdd.map((_, 1))

    val tuples = Array((1, 1), (2, 2), (3, 3))
    val pairRdd01: RDD[(Int, Int)] = sc.parallelize(tuples)

    println("partition:"+numRdd.getNumPartitions)
    numRdd.foreach(println(_))


    //2.使用外部存储系统  本地   hdfs
//    sc.textFile("hdfs://jinghang01:9000/home/spark.txt")
    val lineRdd: RDD[String] = sc.textFile("E://file/spark/spark.txt")

    sc.stop()
  }
}

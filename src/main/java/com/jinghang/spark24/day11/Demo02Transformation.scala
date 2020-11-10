package com.jinghang.spark24.day11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/28
 * desc:
 */
object Demo02Transformation {

  def map(sc: SparkContext) = {
    //创建一个1-10数组的RDD，将所有元素*2形成新的RDD
    val numRdd = sc.parallelize(1 to 10)

    numRdd.map(num => num*2)
    numRdd.map(_*2).foreach(println(_))

  }

  //作用：类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] => Iterator[U]
  def mapPartition(sc: SparkContext) = {
    val numRdd = sc.parallelize(1 to 10)

    val resRdd = numRdd.mapPartitions(its => its.map(_*2) )
    resRdd.foreach(println(_))
    println("---------")
    resRdd.foreachPartition(println(_))

  }

  def mapPartitionWithIndex(sc: SparkContext) = {

    val numRdd = sc.parallelize(1 to 10)
    numRdd.mapPartitionsWithIndex((index,its)=> its.map(index+"-"+_) )
      .foreach(println(_))

  }

  def flatmap(sc: SparkContext) = {

    val arr = Array("a b c d", "a a", "d d")
    //Scala的api
    arr.map(_.split(" "))
      .flatten

    val arrRdd = sc.parallelize(arr)

    val value: RDD[Array[String]] = arrRdd.map(_.split(" "))

    val value1: RDD[String] = arrRdd.flatMap(_.split(" "))
  }

  def glom(sc: SparkContext) = {

    val numRdd = sc.parallelize(1 to 10,3)
    //将每个分区的元素形成一个数组
    val value: RDD[Array[Int]] = numRdd.glom()

    //将excutor 计算的结果以数组的形成返回到driver
    println(value.collect().toBuffer)

  }

  def groupby(sc: SparkContext) = {
    val arr = Array("a b c d", "a a", "d d")
    val rdd = sc.parallelize(arr)
    val wordRdd = rdd.flatMap(_.split(" "))

    val pairRdd = wordRdd.map((_, 1))

    val value: RDD[(String, Iterable[(String, Int)])] = pairRdd.groupBy(_._1)

    value.foreach(println(_))
    println("---------")

    val value1: RDD[(String, Iterable[Int])] = pairRdd.groupByKey()
    value1.foreach(println(_))

  }

  def filter(sc: SparkContext) = {
    val numRdd = sc.parallelize(1 to 10,3)

    //保留返回为true
    numRdd.filter( _%2 == 0)
      .foreach(println(_))

  }

  //
  def sample(sc: SparkContext) = {
    val numRdd = sc.parallelize(1 to 10)
    /*
     withReplacement: 有无放回
      fraction: 抽到的概率  0-1
      seed:

     */
    numRdd.sample(true,0.8)
      .foreach(println(_))


  }

  def distinct(sc: SparkContext) ={

    sc.parallelize(Array(1,1,1,2,2,2,3,3,3,4,5))
      .distinct(1)
      .foreach(println(_))
  }

  def repartition(sc: SparkContext) = {

    val numRdd = sc.parallelize(1 to 10,5)
    println(numRdd.getNumPartitions)

    //一个分区 对应一个task  分区个数绝对并行度
    println( numRdd.repartition(8).getNumPartitions)
    println( numRdd.repartition(3).getNumPartitions)

    /*
      def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
        coalesce(numPartitions, shuffle = true)
      }
     */
    println(numRdd.coalesce(8,true).getNumPartitions)  //
    println(numRdd.coalesce(3).getNumPartitions)

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

//    map(sc)
//    mapPartition(sc)
//    mapPartitionWithIndex(sc)
//    flatmap(sc)
//    glom(sc)
//    groupby(sc)
//    filter(sc)
//    sample(sc)
//    distinct(sc)
    repartition(sc)

    sc.stop()
  }
}

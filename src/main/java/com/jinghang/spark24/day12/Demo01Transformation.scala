package src.main.java.com.jinghang.spark24.day12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo01Transformation {

  def sortBy(sc: SparkContext) = {
    val numRdd = sc.parallelize(1 to 10,5)

    numRdd.sortBy(num => num,false,1)
      .foreach(println(_))

    numRdd.sortBy(num => num,false).repartition(1)
      .foreach(println(_))

    numRdd.map(num => (num,num))
      .sortByKey(false,1)
  }

  def union(sc: SparkContext) = {

    val arr1 = Array(1, 2, 3, 4, 5)
    val numRdd01 = sc.parallelize(arr1)

    val arr2 = Array( 3, 4, 5,6,7)
    val numRdd02 = sc.parallelize(arr2)

    //两个Rdd 合并
    val value: RDD[Int] = numRdd01.union(numRdd02)
    //计算差
    val value1 = numRdd01.subtract(numRdd02)
    //交集
    val value2 = numRdd01.intersection(numRdd02)

    println("----------------")
    //笛卡尔积
    val value3 = numRdd01.cartesian(numRdd02)
    println(numRdd01.getNumPartitions)
    println(numRdd02.getNumPartitions)
    println(value3.getNumPartitions)
    println("----------------")
    //zip 拉链操作  匹配元素个数相等
    val value4 = numRdd01.zip(numRdd02)

//    println(value4.collect().toBuffer)

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

//    sortBy(sc)
    union(sc)

    sc.stop()
  }
}

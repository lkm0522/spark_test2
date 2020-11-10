package src.main.java.com.jinghang.spark24.day14

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo01Accumulator {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

//    var num = 100
//    println(s"driver: $num")
//    val numRdd = sc.parallelize(Array(1, 2, 3, 4, 5))
//    numRdd.map(i => {
//      //在excutor 计算
//      num += i
//      println(s"excutor num: $num")
//      num
//    }).collect()
//    println(s"total Num : $num")

    //声明变量
    val sumAccumulator = sc.longAccumulator("sum")
    println(s"sumAccumulator.value: ${sumAccumulator.value} ")
    val numRdd = sc.parallelize(Array(1, 2, 3, 4, 5))

    numRdd.map(i=>{
      sumAccumulator.add(i)
    }).collect()

    println(s"sumAccumulator : ${sumAccumulator.value} ")

    sc.stop()
  }
}

package src.main.java.com.jinghang.spark24.day14

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo02DefindAccumulator {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)


    val accum = new LogAccumulator
    sc.register(accum, "logAccum")

    //过滤掉带字母的
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2)
    .filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)  //连续数字的都匹配上
      if (!flag) {
        accum.add(line)  //带字母的都添加到累加器
      }
      flag  //返回为true  都留下了
    }).map(_.toInt).reduce(_ + _)
    println("sum: " + sum)
    println()

    println(accum)
    sc.stop()
  }
}

class SumAccumulator extends AccumulatorV2[Int,Int]{

  var sum = 0
  var count = 0

  override def isZero: Boolean = true

  override def copy(): AccumulatorV2[Int, Int] = {
    val accumulator = new SumAccumulator
    accumulator.sum = sum
    accumulator
  }

  override def reset(): Unit = {
    sum = 0
  }

  override def add(v: Int): Unit = {
    sum += v
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    sum += other.value
  }

  override def value: Int = sum
}


class LogAccumulator extends AccumulatorV2[String,java.util.Set[String]]{
  //作为返回值
  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()
  override def isZero: Boolean = _logArray.isEmpty
  //复制对象
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val accumulator = new LogAccumulator
    accumulator._logArray.addAll(_logArray)
    accumulator
  }
  override def reset(): Unit = _logArray.clear()
  override def add(v: String): Unit = {
    _logArray.add(v)
  }
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    _logArray.addAll(other.value)
  }
  override def value: util.Set[String] = _logArray
}
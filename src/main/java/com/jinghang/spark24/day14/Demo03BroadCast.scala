package src.main.java.com.jinghang.spark24.day14

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo03BroadCast {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val num = 100
      //使用广播变量
    val broadcastValue: Broadcast[Int] = sc.broadcast(num)

    //对广播变量的取值
    println(broadcastValue.value)

    sc.stop()
  }
}

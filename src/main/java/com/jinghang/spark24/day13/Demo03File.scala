package src.main.java.com.jinghang.spark24.day13

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo03File {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    sc.textFile("E://file/spark/logs.txt")
    sc.textFile("E://file/spark/people.json")
        .saveAsObjectFile("E://file/spark/out/1030")


    sc.stop()
  }
}

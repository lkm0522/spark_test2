package src.main.java.com.jinghang.spark24.day12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo03Seri {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "jinghang"))

    val search = new Search("a")
    search.getMatch(rdd)
        .foreach(println(_))

    sc.stop()
  }
}

class Search(str:String) extends Serializable {

  def getMatch(rdd: RDD[String]): RDD[String] ={
    rdd.filter(isMatch)
  }

  def isMatch(q:String): Boolean ={
    q.contains(str)
  }


}

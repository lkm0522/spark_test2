package com.jinghang.spark24.day20

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Create By 木木 On 2020.11.08 13:25 星期日
 * Desc:
 */
object Demo01Json
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lineRDD = sc.textFile("E://file/spark/json_data/test.json")

    lineRDD.map(line=>{
      //{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1","name":"海绵宝宝"}
      val jSONObject = JSON.parseObject(line)
      val movie = jSONObject.getInteger("movie")
      val rate = jSONObject.getInteger("rate")
      val timeStamp = jSONObject.getString("timeStamp")
      val uid = jSONObject.getInteger("uid")
      val name = jSONObject.getString("name")
      (movie,rate,timeStamp,uid,name)
    }).foreach(println(_))

    sc.stop()
  }
}

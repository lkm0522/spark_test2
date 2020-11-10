package com.jinghang.spark24.day20

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Create By 木木 On 2020.11.08 14:11 星期日
 * Desc:
 */
object Demo02Json
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lineRdd = sc.textFile("E://file/spark/json_data/applog.json")
    lineRdd.map(line =>
    {
      val jsonObject: JSONObject = JSON.parseObject(line)

      val jsonObject1: JSONObject = jsonObject.getJSONObject("header")
      val city = jsonObject1.getString("city")
      val net_type = jsonObject1.getInteger("net_type")
      val android_id = jsonObject1.getString("android_id")

      (city, net_type, android_id)
    })
      .foreach(println(_))

    sc.stop()
  }
}

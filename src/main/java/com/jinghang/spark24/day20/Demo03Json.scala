package com.jinghang.spark24.day20

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo03Json {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lineRdd = sc.textFile("E://file/spark/json_data/movie.json")

    lineRdd.map(line=>{

      val jSONArray = JSON.parseArray(line)
//      jSONArray.size()

      //取数组的第一个值
      val jsonObject = jSONArray.getJSONObject(0)

      val title = jsonObject.getString("title")
      val types = jsonObject.getJSONArray("types")
      val url = jsonObject.getString("url")
      val is_playable = jsonObject.getBoolean("is_playable")

      (title,types.getString(0),url,is_playable)
    })
        .foreach(println(_))

    sc.stop()
  }
}

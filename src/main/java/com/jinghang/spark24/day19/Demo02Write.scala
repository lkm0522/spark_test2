package com.jinghang.spark24.day19

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo02Write {

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()


    val df = spark.read.json("E://file/spark/people.json")

    //保存
//    df.write.json("E://file/spark/out/1")
//    df.write.save("E://file/spark/out/2")

//    df.write.csv("")
//    df.write.jdbc()
//    df.write.orc("")
//    df.write.parquet("")

    //json  csv  parquet  text jdbc orc
    df.write.format("parquet")
      /*
      Append,
      Overwrite,
      ErrorIfExists,
      Ignore
       */
      .mode(SaveMode.Append).save("E://file/spark/out/1")



    spark.stop()
    sc.stop()
  }
}

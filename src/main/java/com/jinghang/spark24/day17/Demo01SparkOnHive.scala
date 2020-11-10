package com.jinghang.spark24.day17

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo01SparkOnHive {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
//    val sc = new SparkContext(conf)

    //spark on  hive  ：读取hive 的数据
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    var sql = "show databases"
    spark.sql(sql)
        .show()


//    val sc: SparkContext = spark.sparkContext
    spark.stop()
  }
}

package com.jinghang.spark24.day19

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo03SparkOnHive {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    var sql = "show databases"
    spark.sql(sql).show()

    //分析
    sql = "create database spark"
//    spark.sql(sql)

//    sql = "drop table if exists spark.stu_info"
//    spark.sql(sql)
//    sql = "drop table if exists spark.stu_score"
//    spark.sql(sql)

    //创建 info 表
    sql =
      """
        |create external table if not exists spark.stu_info(
        |name string,
        |age int
        |)
        |row format  delimited fields terminated by ','
        |lines terminated by '\n'
        |stored as textfile
        |location "/home/hadoop/data/hivedata/info"
        |""".stripMargin
//    spark.sql(sql)


    //创建 scocre 表
    sql =
      """
        |create external table if not exists spark.stu_score(
        |name string,
        |score int
        |)
        |row format delimited fields terminated by ','
        |lines terminated by '\n'
        |stored as textfile
        |location "/home/hadoop/data/hivedata/score"
        |""".stripMargin
//    spark.sql(sql)

//    //插入数据
//    sql = "load data  inpath '/home/hadoop/data/hivedata/student_infos.txt' into table spark.stu_info"
//    spark.sql(sql)
//    sql = "load data  inpath '/home/hadoop/data/hivedata/student_scores.txt' into table spark.stu_score"
//    spark.sql(sql)

    //需求分析：

    sql =
      """
        |select info.name,info.age,score.score
        |from spark.stu_info as info
        |join spark.stu_score as score
        |on info.name = score.name
        |""".stripMargin

    spark.sql(sql)
        .show()

    spark.stop()
    sc.stop()
  }
}

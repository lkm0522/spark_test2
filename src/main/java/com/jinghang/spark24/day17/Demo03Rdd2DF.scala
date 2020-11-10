package com.jinghang.spark24.day17

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo03Rdd2DF {

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

    import spark.implicits._

    val lineRdd = sc.textFile("E://file/spark/student01.txt")

//    val df: DataFrame = lineRdd.map(line => {
//      //class01 tom 100
//      val splited = line.split(" ")
//      (splited(0), splited(1), splited(2).toInt)
//    }).toDF("classess","name","score")


    val df: DataFrame = lineRdd.map(line => {
      //class01 tom 100
      val splited = line.split(" ")
      Stu(splited(0), splited(1), splited(2).toInt)
    }).toDF()


//    val rdd: RDD[Row] = df.rdd
//    val d: JavaRDD[Row] = df.javaRDD

    //注册成临时表  写sql  分析计算
    df.show()


    spark.stop()
    sc.stop()
  }
}

//构建样例类
case class Stu(classess:String,name:String,score:Int)
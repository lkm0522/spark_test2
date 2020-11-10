package com.jinghang.spark24.day17

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo02SparkSql {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    //原始
//    val sQLContext = new SQLContext(sc)
//    val hiveContext = new HiveContext(sc)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
//      .enableHiveSupport()
      .getOrCreate()

    //创建DataFrame
//    val ds: Dataset[String] = spark.read.textFile("E://file/spark/people.json")
//    val df: DataFrame = ds.toDF()

    val df: DataFrame = spark.read.json("E://file/spark/people.json")
    //输出元数据
//    df.printSchema()
//    df.show()

    //API
//    df.select("name","age").show()
//    df.select("name","age").where(df.col("age") > 20).show()
//    df.select(df.col("name").as("newName"), (df.col("age")+100).as("newAge")).show()
//    df.select("name","age").groupBy("name").sum("age").show()

    //注册临时
//    df.createOrReplaceTempView("p")

    df.createOrReplaceGlobalTempView("p")

    var sql = "select name,sum(age) from p group by name"

    sql =
      """
        |select name,sum(age)
        |from global_temp.p
        |group by name
        |""".stripMargin

    spark.sql(sql).show()



    spark.stop()
  }
}

package src.main.java.com.jinghang.spark24.day13

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo04Mysql {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)


    //3.定义连接mysql的参数
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/spark?serverTimezone=UTC"
    val userName = "root"
    val passWd = "123456"

    //创建JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from `student` where `id`>=? and `id`<=?;",
      1,
      3,
      1,
      r => (r.getInt(1), r.getString(2))
    )

    rdd.foreach(println(_))

    //写入mysql
    val data = sc.parallelize(List(("lili","Female"), ("tom","Male"),("linda","Female")))
    data.foreachPartition(insertData)

    sc.stop()
  }

  def insertData(iterator: Iterator[(String,String)]): Unit = {
    Class.forName ("com.mysql.cj.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?serverTimezone=UTC", "root", "root")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into student(name,gender) values (?,?)")
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.executeUpdate()
    })
  }

}

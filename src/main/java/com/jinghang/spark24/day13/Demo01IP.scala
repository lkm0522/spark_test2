package com.jinghang.spark24.day13

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo01IP {

  def ip2Long(ip: String): Long = {

    val splited = ip.split("\\.")
    //1.1.0.0
    splited(3).toLong*1+splited(2).toLong*256+splited(1).toLong*256*256+splited(0).toLong*256*256*256
  }

  def search(ipRules: Array[(Long, Long, String)], ipLong: Long): Int = {

    var start = 0
    var end = ipRules.length-1
    var mid = 0

    while (start <= end){
      mid = (start+end)/2

      if( ipLong >= ipRules(mid)._1 && ipLong <= ipRules(mid)._2 ){
        return  mid
      }else if (ipLong > ipRules(mid)._2){
        start = mid + 1
      }else if(ipLong <  ipRules(mid)._1){
        end = mid - 1
      }
    }
    -1  //没有找到 返回 -1
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //加载字典数据
    lazy val ipArr: Array[(Long, Long, String)] = sc.textFile("E://file/spark/ip.txt")
      .map(line => {
        val splited = line.split("\\|")
        (splited(2).toLong, splited(3).toLong, splited(6))
      }).collect()

    //使用广播变量优化
    val ipBroadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipArr)

    val lineRdd = sc.textFile("E://file/spark/logs.txt")
      .repartition(3)  //增大分区 提高并行度
    //缓存
    lineRdd.cache()

    lineRdd.map(line=>{
      val splited = line.split("\\|")
      val ip = splited(1)
      //根据ip 求省份

      //将ip转换成十进制数
      val ipLong = ip2Long(ip)

      //广播变量取值
      val ipRules: Array[(Long, Long, String)] = ipBroadcast.value
      //使用二分查找法  查找IP的位置
      val index = search(ipRules, ipLong)
      val province = ipRules(index)._3

      //（省份，1）
     if(index == -1){
       ("未知",1)
     }else{
       (province,1)
     }
    })
      .reduceByKey(_+_)
      .sortBy(_._2,false,1)  //排序  降序 重分区
      .take(3)
      .foreach(println(_))

    sc.stop()
  }
}

package src.main.java.com.jinghang.spark24.day13

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo05Hbase {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //构建HBase配置信息
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node7-1,node7-2,node7-3")
    hconf.set(TableInputFormat.INPUT_TABLE, "spark")

    //创建表  create 'spark','cf'
    /*插入数据
    put 'spark','1','cf:id','1'
    put 'spark','1','cf:name','laowang'

    put 'spark','2','cf:id','2'
    put 'spark','2','cf:name','laowang2'

    put 'spark','3','cf:id','3'
    put 'spark','3','cf:name','laowang3'

    put 'spark','4','cf:id','4'
    put 'spark','4','cf:name','laowang4'

    put 'spark','5','cf:id','5'
    put 'spark','5','cf:name','laowang5'
     */

    //从HBase读取数据形成RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hconf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count: Long = hbaseRDD.count()
    println(count)

    //对hbaseRDD进行处理
    hbaseRDD.foreach {
      case (_, result) =>
        val key: String = Bytes.toString(result.getRow)
        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id")))
        val color: String = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name")))
        println("RowKey:" + key + ",Name:" + name + ",Color:" + color)
    }

    sc.stop()
  }
}

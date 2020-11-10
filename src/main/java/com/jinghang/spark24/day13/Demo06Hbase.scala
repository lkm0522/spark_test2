package src.main.java.com.jinghang.spark24.day13

import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo06Hbase {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)


    //创建HBaseConf
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node7-1,node7-2,node7-3")

    val jobConf = new JobConf(hconf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "spark")

    //构建Hbase表描述器
    val fruitTable = TableName.valueOf("spark")
    val tableDescr = new HTableDescriptor(fruitTable)
    tableDescr.addFamily(new HColumnDescriptor("cf".getBytes))

    //创建Hbase表
//    val admin = new HBaseAdmin(hconf)
//    if (admin.tableExists(fruitTable)) {
//      admin.disableTable(fruitTable)
//      admin.deleteTable(fruitTable)
//    }
//    admin.createTable(tableDescr)

    //定义往Hbase插入数据的方法
    def convert(triple: (String, String, String)) = {
      val put = new Put(Bytes.toBytes(triple._1))
      put.addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(triple._2))
      put.addImmutable(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, put)
    }

    //创建一个RDD
    val initialRDD = sc.parallelize(List(("11","11","apple"), ("12","12","banana"), ("13","13","pear")))

    //将RDD内容写到HBase
    val localData = initialRDD.map(convert)
    localData.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}

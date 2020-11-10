package src.main.java.com.jinghang.spark24.day12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo04Cache {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("E://file/spark/out")

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "jinghang"))

    rdd.cache()

    /**
     * Persist this RDD with the default storage level (`MEMORY_ONLY`).
     */
//    def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)
    /*
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(true, true, true, false, 1)
     */
    rdd.persist(StorageLevel.MEMORY_ONLY)


    //为了防止节点挂掉 导致数据丢失
    rdd.checkpoint()

    sc.stop()
  }
}

package src.main.java.com.jinghang.spark24.day12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * create by young
 * date:20/10/26
 * desc:
 */
object Demo02KVRDD {

  def partitionBy(sc: SparkContext) = {
    val pairRdd = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)

    pairRdd.partitionBy(new org.apache.spark.HashPartitioner(2))

    val rdd01 = pairRdd.partitionBy(new MyPartitioner(2))
    println(rdd01.getNumPartitions)
  }

  def groupBykey(sc: SparkContext) = {

    val lineRdd = sc.textFile("E://file/spark/student01.txt")
    val rdd01: RDD[(String, Int)] = lineRdd.map(line => {
      //class01 tom 100
      val splited = line.split(" ")

      (splited(0), splited(2).toInt)
    })

    val groupedRdd: RDD[(String, Iterable[Int])] = rdd01.groupByKey()

    groupedRdd.map(tuple=>{
      val classess = tuple._1
      val sum = tuple._2.toList.sum

//      var sum1 = 0L
//      var count = 0
//      val it = tuple._2.iterator
//      while (it.hasNext){
//        sum1 += it.next()
//        count += 1
//      }

      (classess,sum)
    })
      .foreach(println(_))

    println("-----------")
    //
    val value: RDD[(String, Int)] = rdd01.reduceByKey(_ + _)
    value.foreach(println(_))

  }

  def reduceBykey(sc: SparkContext) = {

    val rdd01 = sc.parallelize(Array("a a b", "a b c", "c c a"), 3)
    val pairRdd = rdd01.flatMap(_.split(" ")).map((_, 1))

    val rdd02 = pairRdd.reduceByKey(_ + _)
    rdd02.foreach(println(_))

    println("-------------")
    //设置每个分区内的初始值
    // _+_ 是每个分区内聚合   _+_ 分区内做的聚合
    val res01 = pairRdd.aggregateByKey(100)(_ + _, _ + _)
    res01.foreach(println(_))

    pairRdd.foldByKey(100)(_+_)

    println("-------------")
    val resRdd02 = pairRdd.combineByKey(num => num + 100, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)
    resRdd02.foreach(println(_))
  }

  def mapValues(sc: SparkContext) = {

    val pairRdd = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    pairRdd.mapValues(_+"GG")
      .foreach(println(_))

  }

  def join(sc: SparkContext) = {

    val lineRdd = sc.textFile("E://file/spark/student01.txt")
    val rdd01: RDD[(String, Int)] = lineRdd.map(line => {
      //class01 tom 100
      val splited = line.split(" ")
      //name  score
      (splited(1), splited(2).toInt)
    })

    val lineRdd02 = sc.textFile("E://file/spark/student02.txt")
    val rdd02: RDD[(String, Int)] = lineRdd02.map(line => {
      //1 jack 18
      val splited = line.split(" ")
      //name age
      (splited(1), splited(2).toInt)
    })

    val joinedRdd: RDD[(String, (Int, Int))] = rdd01.join(rdd02)

    joinedRdd.map(tuple => {
      val name = tuple._1
      val score = tuple._2._1
      val age = tuple._2._2
      (name,score,age)
    }).foreach(println(_))


    val value: RDD[(String, (Int, Option[Int]))] = rdd01.leftOuterJoin(rdd02)
    //取值自己做一下

    val value1: RDD[(String, (Option[Int], Option[Int]))] = rdd01.fullOuterJoin(rdd02)


    //作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
    val value2: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd01.cogroup(rdd02)

    //count
    val l: Long = rdd01.count()
    val tuple: (String, Int) = rdd01.first()
    val tuples = rdd01.take(3)

    val stringToLong: collection.Map[String, Long] = rdd01.countByKey()


  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

//    partitionBy(sc)
//    groupBykey(sc)
//    reduceBykey(sc)
//    mapValues(sc)
//    join(sc)

    val rdd = sc.parallelize(Array(2,5,4,6,8,3))
    println(rdd.takeOrdered(3).reverse.toBuffer)
//    rdd.saveAsTextFile("")
//    rdd.saveAsObjectFile()

    sc.stop()
  }
}

//自定义分区
class MyPartitioner(num:Int) extends Partitioner{
  override def numPartitions: Int = num
  override def getPartition(key: Any): Int = {
    key.hashCode()%num
  }
}


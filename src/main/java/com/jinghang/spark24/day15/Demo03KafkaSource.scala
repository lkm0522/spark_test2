package com.jinghang.spark24.day15

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Create By 木木 On 2020.11.05 15:54 星期四
 * Desc:
 */
object Demo03KafkaSource
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //指定batch时间
    val ssc = new StreamingContext(sc, Seconds(5))

    val topics = List("test01")

    val kafkaParams: Map[String, String] = Map(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node7-2:9092"
    )


    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    dstream.map(record =>
    {
      //      record.checksum()
      //      record.key()
      //      record.offset()  //拿到偏移量 可以自己维护
      //      record.partition()
      //      record.serializedKeySize()
      //      record.serializedValueSize()
      //      record.timestamp()
      //      record.topic()
      record.value()
    })
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

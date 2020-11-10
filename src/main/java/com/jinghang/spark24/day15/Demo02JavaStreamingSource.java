package com.jinghang.spark24.day15;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Int;
import scala.Mutable;

import java.util.Queue;

/**
 * Create By 木木 On 2020.11.04 11:31 星期三
 * Desc:
 */
public class Demo02JavaStreamingSource
{
    public static void main(String[] args) throws InterruptedException
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("Demo01JavaStreamingWC").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));

        jssc.start();

        jssc.awaitTermination();

    }
}


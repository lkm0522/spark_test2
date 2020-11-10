package com.jinghang.spark24.day15;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Create By 木木 On 2020.11.04 11:10 星期三
 * Desc:
 */
public class Demo01JavaStreamingWc
{
    public static void main(String[] args) throws InterruptedException
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("Demo01JavaStreamingWC").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));

        JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream("ndoe7-1", 9999);

        inputDStream.flatMap(new FlatMapFunction<String, String>()
        {
            @Override
            public Iterator<String> call(String line) throws Exception
            {
                return Arrays.asList(line.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>()
        {
            @Override
            public Tuple2<String, Integer> call(String words) throws Exception
            {
                return new Tuple2<>(words,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>()
        {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception
            {
                return v1 + v2;
            }
        }).print();

        jssc.start();
        jssc.awaitTermination();
    }
}

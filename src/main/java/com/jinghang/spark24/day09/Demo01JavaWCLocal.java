package com.jinghang.spark24.day09;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * create by young
 * date:20/10/26
 * desc:
 */
public class Demo01JavaWCLocal {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("Demo01JavaWCLocal").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lineRdd = jsc.textFile("E://file/spark/spark.txt");

        //使用表达式
//        lineRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<>(word,1))
//                .reduceByKey((a,b) -> a+b)
//                .foreach(res -> System.out.println(res));

        lineRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] splited = line.split(" ");
                return Arrays.asList(splited).iterator();
            }
        })
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<>(word,1);
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                })
                .foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> tuple) throws Exception {
                        System.out.println(tuple._1+"  "+tuple._2);
                    }
                });
        jsc.stop();
    }
}
package com.jinghang.spark24.day11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

/**
 * create by young
 * date:20/10/28
 * desc:
 */
public class Demo01JavaCreateRdd {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Demo01JavaCreateRdd").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //从内部集合
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numRdd = jsc.parallelize(list);

        JavaPairRDD<Integer, Integer> pairRdd = numRdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i) throws Exception {
                return new Tuple2<>(i, i);
            }
        });

        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(
                new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(1, "b"),
                new Tuple2<Integer, String>(1, "v")
        );
        JavaPairRDD<Integer, String> pairRdd01 = jsc.<Integer, String>parallelizePairs(tuple2s);

        List<Tuple2<String, Double>> arr2 = Arrays.asList(
                new Tuple2<String, Double>("u1", 20.01),
                new Tuple2<String, Double>("u2", 18.95),
                new Tuple2<String, Double>("u3", 20.55),
                new Tuple2<String, Double>("u4", 20.12),
                new Tuple2<String, Double>("u5", 100.11)
        );
        JavaPairRDD<String, Double> rdd2 = jsc.<String, Double>parallelizePairs(arr2);

        //从存储系统  本地  hdfs  hbase
        JavaRDD<String> lineRdd = jsc.textFile("E://file/a.txt");

        jsc.stop();

    }
}

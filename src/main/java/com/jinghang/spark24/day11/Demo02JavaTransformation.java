package com.jinghang.spark24.day11;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * create by young
 * date:20/10/28
 * desc:
 */
public class Demo02JavaTransformation {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("Demo01JavaCreateRdd").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

//        map(jsc);
//        mapPartition(jsc);
//        mapPartitionWithIndex(jsc);
//        flatMap(jsc);
        groupBy(jsc);


        jsc.stop();

    }

    private static void groupBy(JavaSparkContext jsc) {
        List<String> arrs = Arrays.asList("a b c d", "a a", "d d");
        JavaRDD<String> arrRdd = jsc.parallelize(arrs);

        //flatMapToPair =  flatMap +  MapToPair
        JavaPairRDD<String, Integer> pairRdd = arrRdd.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String line) throws Exception {

                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();

                String[] splited = line.split(" ");
                for (String word : splited) {
                    list.add(new Tuple2<>(word, 1));
                }

                return list.iterator();
            }
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupedRdd01 = pairRdd.groupBy(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple._1;
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupedRdd = pairRdd.groupByKey();


    }

    private static void flatMap(JavaSparkContext jsc) {

        List<String> arrs = Arrays.asList("a b c d", "a a", "d d");
        JavaRDD<String> arrRdd = jsc.parallelize(arrs);

        JavaRDD<String> wordRdd = arrRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        wordRdd.foreach(it -> System.out.println(it));
    }

    private static void mapPartitionWithIndex(JavaSparkContext jsc) {

        JavaRDD<Integer> numRdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),3);

        JavaRDD<String> resrdd = numRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> it) throws Exception {

                //index  分区编号
                //it 是每个分区的数据
                ArrayList<String> list = new ArrayList<>();
                while (it.hasNext()) {
                    Integer next = it.next();
                    list.add(index + "-" + next);
                }
                return list.iterator();
            }
        }, true);

        resrdd.foreach(ele -> System.out.println(ele));

    }

    private static void mapPartition(JavaSparkContext jsc) {

        JavaRDD<Integer> numRdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        numRdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> it) throws Exception {

                ArrayList<Integer> list = new ArrayList<>();
                //it 代表整个分区的数据
                while (it.hasNext()){
                    list.add(it.next()*2);
                }
                return list.iterator();
            }
        })
                .foreachPartition(new VoidFunction<Iterator<Integer>>() {
                    @Override
                    public void call(Iterator<Integer> iterator) throws Exception {
                        while (iterator.hasNext())
                            System.out.println(iterator.next());
                    }
                });

//                .foreach(ele -> System.out.println(ele));
    }

    private static void map(JavaSparkContext jsc) {

        //使用默认分区个数  2
        JavaRDD<Integer> numRdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        numRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) throws Exception {
                return num*2;
            }
        }).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}

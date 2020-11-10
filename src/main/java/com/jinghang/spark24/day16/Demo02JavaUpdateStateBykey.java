package com.jinghang.spark24.day16;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * create by young
 * date:20/10/28
 * desc:
 */
public class Demo02JavaUpdateStateBykey {

    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("Demo01JavaCreateRdd").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));

        jssc.checkpoint("E://file/spark/1103");

        JavaReceiverInputDStream<String> dStream = jssc.socketTextStream("node7-1", 9999);

        dStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> options) throws Exception {
                        //values  是当前batch 的数据
                        //options 是前面batch 累计的结果
                        int sum = 0;
                        Iterator<Integer> iterator = values.iterator();
                        while (iterator.hasNext()){
                            sum += iterator.next();
                        }
                        sum += options.orElse(0);
                        return Optional.of(sum);
                    }
                }).print();

       jssc.start();
       jssc.awaitTermination();

    }
}

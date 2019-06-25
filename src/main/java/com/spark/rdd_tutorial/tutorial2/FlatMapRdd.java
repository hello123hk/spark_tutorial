package com.spark.rdd_tutorial.tutorial2;

import com.spark.rdd.tutorial.util.Constant;
import com.spark.rdd.tutorial.util.MyIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * Created by zhaikaishun on 2017/8/20.
 * 2.0 版本以上的用iterator
 */
public class FlatMapRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("FlatMapRdd").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        JavaRDD<String> lines = jsc.textFile(Constant.filePath);

        JavaRDD<String> flatMapRDD = (JavaRDD<String>) lines.flatMap((FlatMapFunction<String,String>) line -> {
            String[] split = line.split("\\s+");
            return new MyIterator<>(Arrays.asList(split));
        });

        flatMapRDD.foreach( (VoidFunction<String>) word -> System.out.println(word));


        JavaRDD<Iterable<String>> mapRDD = (JavaRDD<Iterable<String>>) lines.map((Function<String,Iterable<String>>) line -> {
            String[] split = line.split("\\s+");
            return Arrays.asList(split);
        });

        mapRDD.foreach((VoidFunction<Iterable<String>>) word -> System.out.println(word));


    }
}

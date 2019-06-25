package com.spark.rdd_tutorial.tutorial2;

import com.spark.rdd.tutorial.util.Constant;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * Created by zhaikaishun on 2017/8/20.
 * 貌似 map 在java 中没多大用处
 */
public class MapRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapRdd").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        JavaRDD<String> lines = jsc.textFile(Constant.filePath);
        JavaRDD<Iterable<String>> mapRDD = (JavaRDD<Iterable<String>>) lines.map((Function<String, Iterable<String>>) s -> {
            String[] split = s.split("\\s+");
            return Arrays.asList(split);
        });
        //循环打印
        mapRDD.foreach((VoidFunction<Iterable<String>>) s -> System.out.println(s));
    }
}

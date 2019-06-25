package com.spark.rdd_tutorial.tutorial1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class ParallelizeRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        JavaRDD<String> javaStringRDD = (JavaRDD<String>) jsc.parallelize(
                Arrays.asList("shenzhen", "is a beautiful city"),1);
        List<String> collect = javaStringRDD.collect();
        for (String str:collect) {
            System.out.println(str);
        }
    }
}

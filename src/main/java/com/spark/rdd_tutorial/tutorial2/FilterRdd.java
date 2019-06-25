package com.spark.rdd_tutorial.tutorial2;

import com.spark.rdd.tutorial.util.Constant;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class FilterRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("FilterRdd").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        JavaRDD<String> lines = jsc.textFile(Constant.filePath);
        JavaRDD<String> zksRDD = lines.filter((Function<String,Boolean>) line -> line.contains("zks"));
        List<String> zksCollect = zksRDD.collect();
        for (String str:zksCollect) {
            System.out.println(str);
        }
    }
}

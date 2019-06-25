package com.spark.rdd_tutorial.tutorial4;

import com.spark.rdd.tutorial.util.Constant;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class FlatMapToPairRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("FlatMapToPairRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile(Constant.filePath);

        JavaPairRDD<String, Integer> wordPairRDD = (JavaPairRDD<String, Integer>) lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) {
                ArrayList<Tuple2<String, Integer>> tpLists = new ArrayList<>();
                String[] split = s.split("\\s+");
                for (int i = 0; i <split.length ; i++) {
                    Tuple2 tp = new Tuple2<>(split[i], 1);
                    tpLists.add(tp);
                }
                return tpLists.iterator();
            }
        });

        wordPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tp) throws Exception {
                System.out.println("key: "+tp._1+" value: "+tp._2);
            }
        });


//        JavaPairRDD<String,Integer> wordPairRdd = (JavaPairRDD<String, Integer>) lines.flatMapToPair((PairFlatMapFunction<String, String, Integer>) s -> {
//            ArrayList<Tuple2<String, Integer>> tpList = new ArrayList<>();
//            String[] split = s.split("\\s+");
//            for (int i = 0; i < split.length; i++) {
//                Tuple2 tp = new Tuple2(split[i], 1);
//                tpList.add(tp);
//            }
//            return tpList.iterator();
//        });

//        wordPairRDD.foreach(
//                (VoidFunction<Tuple2<String, Integer>>) tp -> System.out.println("key: "+tp._1+" value: "+tp._2)
//        );
    }
}

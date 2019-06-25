package com.spark.rdd_tutorial.tutorial10;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/4/18.
 */
public class CollectAsMapRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("CollectAsMapRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaRDD<Tuple2<Integer, Integer>> tupleRDD =
                sc.parallelize(Arrays.asList(new Tuple2<>(1, 2),
                        new Tuple2<>(2, 4),
                        new Tuple2<>(2, 5),
                        new Tuple2<>(3, 4),
                        new Tuple2<>(3, 5),
                        new Tuple2<>(3, 6)));

        JavaPairRDD<Integer, Integer> mapRDD = (JavaPairRDD<Integer, Integer>) JavaPairRDD.fromJavaRDD(tupleRDD);

        JavaPairRDD<Integer, Integer> wordCountRDD = mapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        // 发现bug， 会报 [Ljava.lang.Object; cannot be cast to [Lscala.Tuple2;
        Map<Integer, Integer> collectMap = wordCountRDD.collectAsMap();

        for (Integer key: collectMap.keySet()){
            System.out.println("key: "+key+" value: "+collectMap.get(key));
        }

    }


}

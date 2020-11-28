package com.virtualpairprogrammers;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        List<String> logData = new ArrayList<>();
        logData.add("WARN: Tuesday 4 September 0405");
        logData.add("ERROR: Tuesday 4 September 0408");
        logData.add("FATAL: Wednesday 5 September 1632");
        logData.add("ERROR: Friday 7 September 1854");
        logData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(logData)
            .mapToPair(rawString -> new Tuple2<>(rawString.split(":")[0], 1L))
            .reduceByKey((val1, val2) -> val1 + val2)
            .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));



        /*  GroupByKey  -- AVOID  because of issues with large datasets.
            It uses google java library Guava for Iterables.size, this allows to obtain
            an element count for an iterable collection
         */
//        sc.parallelize(logData)
//                .mapToPair(rawString -> new Tuple2<>(rawString.split(":")[0], 1L))
//                .groupByKey()
//                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));


//        ===========================================================================================

//        List<Integer> inputData = new ArrayList<>();
//        inputData.add(35);
//        inputData.add(12);
//        inputData.add(90);
//        inputData.add(20);
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<Integer> originalInt = sc.parallelize(inputData);
//
//        // Scala tuples
//        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalInt.map(value -> new Tuple2<>(value, Math.sqrt(value)));

//      Tuple2<Integer, Double> myValue = new Tuple2<>(9, 3.0);

//        ==========================================================================================

//        JavaRDD<Integer> myRdd = sc.parallelize(inputData);
//        //reducing
//        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);
//        //mapping
//        JavaRDD<Double> sqrtRdd = myRdd.map( value -> Math.sqrt(value) );
//
////        sqrtRdd.foreach( value -> System.out.println(value) );
//        /*
//        newer syntax to achieve the same printout  " System.out::println "
//        collect() method is chained to convert Rdd into a List, and forEach() is used because that
//        is the corresponding iterator for Lists. this was done to prevent a not serializable exception
//        in machines with more than one physical core, bc println is not serializable.
//         */
//        sqrtRdd.collect().forEach( System.out::println );
//
//        System.out.println(result);
//
//        // counting elements in Rdd using map and reduce
//        JavaRDD<Long> singleElements = sqrtRdd.map(value -> 1L);
//        Long count = singleElements.reduce((val1, val2) -> val1 + val2);
//        System.out.println(count);


        sc.close();


    }


}

package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        //reducing
        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);
        //mapping
        JavaRDD<Double> sqrtRdd = myRdd.map( value -> Math.sqrt(value) );

//        sqrtRdd.foreach( value -> System.out.println(value) );
        /*
        newer syntax to achieve the same printout  " System.out::println "
        collect() method is chained to convert Rdd into a List, and forEach() is used because that
        is the corresponding iterator for Lists. this was done to prevent a not serializable exception
        in machines with more than one physical core, bc println is not serializable.
         */
        sqrtRdd.collect().forEach( System.out::println );

        System.out.println(result);

        sc.close();


    }


}

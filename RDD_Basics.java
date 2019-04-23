package com.Spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;

public class RDD_Basics {
  public static void main(String[]args) {
	  SparkConf conf = new SparkConf().setAppName("RDD_Basics").setMaster("local");
	  JavaSparkContext context = new JavaSparkContext(conf);
	  JavaRDD<String> rdd = context.textFile("/home/abhilash/Downloads/ml-1m/movies.dat");
	  JavaRDD<String> rdd1 = rdd.filter(a -> a.contains("Comedy"));
	  JavaRDD<Integer> rdd3 = rdd1.map(new MyFunctionClass());
//	  JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split("::")).iterator());
//	  JavaPairRDD<Integer, String> rdd2 = rdd1.mapToPair(s -> new Tuple2(rdd1[0],));
//	  System.out.println("Total no of records in movies table:"+rdd.count());
	  rdd3.saveAsTextFile("/home/abhilash/Desktop/rddfile3");
//	  System.out.println(rdd2.collect());
  }
}

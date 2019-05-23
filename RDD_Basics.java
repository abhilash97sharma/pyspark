package org.spark;

import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class RDD_Basics {
   public static void main(String[]args) {
	   SparkConf conf = new SparkConf().setAppName("RDD_Basics").setMaster("local");
	   JavaSparkContext context = new JavaSparkContext(conf);
//	   String[]str = {"hello","abhilash","hello","hi","hello"};
	   JavaRDD<String> rdd = context.textFile("/home/abhilash/Documents/files/file");
//	   JavaRDD<String>
//	   JavaRDD<String> rdd = context.parallelize(Arrays.asList(str));
//	   JavaPairRDD<String,Integer> rdd1 = rdd.mapToPair(word -> new Tuple2<String,Integer>(word,1)).reduceByKey((a,b) -> a+b);
//	   rdd1.saveAsTextFile("/home/abhilash/Documents/rddfile");
	
	   /*  for creating a user define function and applying certain transformation.
	   JavaPairRDD<String,Character> rdd2 = rdd.mapToPair(word -> new Tuple2<String,Character>(word,User_define.first(word)));  
	   rdd2.saveAsTextFile("/home/abhilash/Documents/rddfile");
	   */
	   
   }
}

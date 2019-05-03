package com.Spark;

import java.util.Arrays;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;


public class SparkSQL {
  public static void main(String[]args) {
	  SparkSession spark = new SparkSession.Builder().master("yarn-client").appName("Demo_Spark").getOrCreate();
      Dataset<Row> df = spark.read().csv("hdfs://192.168.5.176:8020/user/root/abhilash/movies.dat");      
      Dataset<Row> user = spark.read().csv("hdfs://192.168.5.176:8020/user/root/abhilash/users.dat");
      SQLContext context = new SQLContext(spark);
      // Structured movies data
      Dataset<Row> df1 = df.withColumn("ID",functions.split(functions.col("_c0"), "::").getItem(0));
      Dataset<Row> df2 = df1.withColumn("Title",functions.split(functions.col("_c0"), "::").getItem(1));
      Dataset<Row> df3 = df2.withColumn("Genres",functions.split(functions.col("_c0"), "::").getItem(2)).drop(functions.col("_c0"));
  //  df3 = df3.filter(functions.col("Genres").contains("Comedy"));  // for filtering a comedy movie
      Dataset<Row> df4 = df3.groupBy(functions.col("Genres")).count().orderBy(functions.col("count").desc()).filter(functions.col("Genres").notEqual("null"));
      
      //Structuring user.dat file
      Dataset<Row> user1 = user.withColumn("UserID",functions.split(functions.col("_c0"), "::").getItem(0));
      Dataset<Row> user2 = user1.withColumn("Gender",functions.split(functions.col("_c0"), "::").getItem(1));
      Dataset<Row> user3 = user2.withColumn("Age",functions.split(functions.col("_c0"), "::").getItem(2));
      Dataset<Row> user4 = user3.withColumn("Occupation",functions.split(functions.col("_c0"), "::").getItem(3));
      Dataset<Row> user5 = user4.withColumn("Zipcode",functions.split(functions.col("_c0"), "::").getItem(4)).drop(functions.col("_c0"));
      
      //counting max no of count on the basis of zipcode
      user5.groupBy(functions.col("Zipcode")).count().orderBy(functions.col("count").desc()).show(5, false);     
    		  
  //    df4.show(5,false);
  //    user5.show(6, false);
      
      // creating your own udf
      spark.udf().register("myname",
    		  (String s) -> s.toUpperCase(), DataTypes.StringType);
      
      //creating udf using UDF1 class
      spark.udf().register("fun12", new UDF1<String,Integer>(){
    	 public Integer call(String sd) throws Exception{
    		 return sd.length();
    	 }
      },DataTypes.IntegerType);
      
      //creating udf with 2 arguments
      spark.udf().register("fun123", 
    		  (String s1,String s2) -> s1+"-"+s1.length(),DataTypes.StringType);
      
      //creating udf with 2 arguments using UDF2 class
      spark.udf().register("fun1234", new UDF2<String,String,String>(){
    	  public String call(String s1, String s2) throws Exception{
    		  return s1+"-"+s2.length();
    	  }
      },DataTypes.StringType);
      
//    df4.select(functions.callUDF("myname",functions.col("Genres"))).show(); //using myname udf function
//    df4.select(functions.col("Genres"),functions.callUDF("fun12", functions.col("Genres")),functions.callUDF("fun123", functions.col("Genres"),functions.col("Genres"))).show();  //using fun12 udf function
//    Dataset<Row> df5 = df4.select(functions.col("Genres"),functions.callUDF("fun1234", functions.col("Genres"), functions.col("Genres")));  // working fine
  }
}

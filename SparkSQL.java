package com.Spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

public class SparkSQL {
  public static void main(String[]args) {
	  SparkSession spark = new SparkSession.Builder().master("local").appName("Demo_Spark").getOrCreate();
      Dataset<Row> df = spark.read().csv("/home/abhilash/Downloads/ml-1m/movies.dat");
  //    Dataset<Row> user = spark.read().csv("/home/abhilash/Downloads/ml-1m/users.dat");
      Dataset<Row> df1 = df.withColumn("ID",functions.split(functions.col("_c0"), "::").getItem(0));
      Dataset<Row> df2 = df1.withColumn("Title",functions.split(functions.col("_c0"), "::").getItem(1));
      Dataset<Row> df3 = df2.withColumn("Genres",functions.split(functions.col("_c0"), "::").getItem(2)).drop(functions.col("_c0"));
  //  df3 = df3.filter(functions.col("Genres").contains("Comedy"));  // for filtering a comedy movie
      Dataset<Row> df4 = df3.groupBy(functions.col("Genres")).count().orderBy(functions.col("count").desc()).filter(functions.col("Genres").notEqual("null"));
      df4.show(5,false);
  //    user.show(6, false);
  }
}

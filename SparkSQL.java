package com.Spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

public class SparkSQL {
  public static void main(String[]args) {
	  SparkSession spark = new SparkSession.Builder().master("local").appName("Demo_Spark").getOrCreate();
      Dataset<Row> df = spark.read().csv("/home/abhilash/Downloads/ml-1m/movies.dat");
  //  Dataset<Row> user = spark.read().csv("/home/abhilash/Downloads/ml-1m/users.dat");
      SQLContext context = new SQLContext(spark);
      Dataset<Row> df1 = df.withColumn("ID",functions.split(functions.col("_c0"), "::").getItem(0));
      Dataset<Row> df2 = df1.withColumn("Title",functions.split(functions.col("_c0"), "::").getItem(1));
      Dataset<Row> df3 = df2.withColumn("Genres",functions.split(functions.col("_c0"), "::").getItem(2)).drop(functions.col("_c0"));
  //  df3 = df3.filter(functions.col("Genres").contains("Comedy"));  // for filtering a comedy movie
      Dataset<Row> df4 = df3.groupBy(functions.col("Genres")).count().orderBy(functions.col("count").desc()).filter(functions.col("Genres").notEqual("null"));
  //    df4.show(5,false);
  //    user.show(6, false);
      
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
      
      
//      df4.select(functions.callUDF("myname",functions.col("Genres"))).show(); //using myname udf function
//      df4.select(functions.col("Genres"),functions.callUDF("fun12", functions.col("Genres")),functions.callUDF(udfName, cols)).show();  //using fun12 udf function
  }
}

package com.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

public class MySQL_Spark {
   public static void main(String[]args) {
	   SparkConf conf = new SparkConf().setAppName("Mysql-spark").setMaster("local");
	   SparkContext sc = new SparkContext(conf);
	   SQLContext context = new SQLContext(sc);
       Dataset<Row> df = context.read().format("jdbc").option("url", "jdbc:mysql://localhost:3306/abhi").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "tb1").option("user", "root").option("password", "root").load();
       df.show();
   }
}

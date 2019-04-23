package com.Spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class Spark_Kafka_consumer {
  public static void main(String[]args) {
	  SparkSession spark = new SparkSession.Builder().master("local").appName("Demo_Spark").getOrCreate();
      Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "192.168.5.133:9092").option("subscribe", "test").load();
 //     Dataset<Row>df1=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
 //     df1.show(5, false);
 /*     StreamingQuery ds = df
    		  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    		  .writeStream()
    		  .outputMode("append")
    		  .format("console")
    		  .option("topic", "test")
    		  .start();*/
      StreamingQuery ds = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    		  .writeStream().outputMode("append").format("csv").option("path","/home/abhilash/Downloads/ml-1m/output")
    		  .option("topic", "test")
    		  .option("checkpointLocation","/home/abhilash/Downloads/ml-1m/output1")
    		  .start();      
      try {
		ds.awaitTermination();
	} catch (StreamingQueryException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
}

package com.Spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Spark_Producer {
	public static void main(String[] args) {
		
		SparkSession spark = new SparkSession.Builder().master("local").appName("Producer").getOrCreate();
		List<StructField> lst = new ArrayList<StructField>();
		String fields = "key value value1";
		for(String s: fields.split(" ")) {
			lst.add(DataTypes.createStructField(s, DataTypes.StringType, true));
		}
		
		StructType schema1 = DataTypes.createStructType(lst);
		Dataset<Row> df = spark.readStream().format("csv").schema(schema1)
				.option("path", "hdfs://192.168.5.176:8020/user/root/pankaj/ml-20m/score/").load();
		StreamingQuery ds = df
				.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","CAST(value1 AS STRING)")
				.writeStream().format("kafka")
				.option("kafka.bootstrap.servers", "192.168.5.133:9092").option("checkpointLocation", "hdfs://192.168.5.176:8020/user/root/abhilash/output4")
				.option("topic", "test").start();
/*		StreamingQuery ds = df.writeStream().outputMode("append").format("csv")
				.option("path","hdfs://192.168.5.176:8020/user/root/abhilash/output2")
				.option("checkpointLocation","hdfs://192.168.5.176:8020/user/root/abhilash/output3")
				.start();*/  //working fine
		try {
			ds.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

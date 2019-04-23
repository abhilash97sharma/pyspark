package com.Spark;
import org.apache.spark.api.java.function.*;

public class MyFunctionClass implements Function{

	@Override
	public Integer call(Object v1) throws Exception {
		// TODO Auto-generated method stub
		String obj1 = (String)v1;
		String []data = obj1.split("::");
		return data.length;
	}
}

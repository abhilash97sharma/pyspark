from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sc = SparkContext("local","Schema")
sqlContext = SQLContext(sc)
schema1 = StructType([StructField("Sentiment",StringType(),True),StructField("text",StringType(),True),StructField("User",IntegerType(),True)])
file = sqlContext.read.option("header","true").schema(schema1).csv('file:///home/abhilash/Downloads/data.csv')
file.printSchema()

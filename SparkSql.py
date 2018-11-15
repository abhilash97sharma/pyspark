from pyspark import SparkContext
from pyspark import SQLContext
sc=SparkContext("local","SparkSql")
sqlContext=SQLContext(sc)
file=sqlContext.read.text("/home/abhilash/Documents/files/file")


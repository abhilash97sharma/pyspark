from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
sc=SparkContext("local","wordcount")
sqlContext = SQLContext(sc)
sparkSession = SparkSession.builder.appName("wordcount").getOrCreate()
file1 = sqlContext.read.text('/Abhilash/file')
file1.show()
file2=file1.rdd
print("objects get created")
map1 = file2.flatMap(lambda w : w.split(" "))
map2=map1.map(lambda w1:(w1,1))
map3=map2.reduceByKey(lambda a,b:a+b)
map3.saveAsTextFile('file:///home/abhilash/Documents/wd.txt')



from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext("local","Reading_Textfile")
file = sc.textFile('hdfs://localhost:9000/Abhilash/file')
map1 = file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
counts = map1.reduceByKey(lambda a, b: a + b)
for i in counts.collect():
    print(i)


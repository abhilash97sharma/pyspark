from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext

sc = SparkContext("local", "Spark_streaming_F")
ssc = StreamingContext(sc,20)
lines = ssc.textFileStream('/home/abhilash/Documents/files/streaming_data')
words = lines.flatMap(lambda s : s.split(" "))
wordcounts = words.map(lambda w : (w,1)).reduceByKey(lambda x,y:x+y)
wordcounts.pprint()
ssc.start()
ssc.awaitTermination()



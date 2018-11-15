from pyspark.streaming import StreamingContext
from pyspark import SparkContext

sc=SparkContext("local","pyspark_streaming")
ssc=StreamingContext(sc,1)
lines=ssc.socketTextStream("localhost",9999)
words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).map(lambda rec: TagCount(rec[0],rec[1])).foreachRDD(lambda rdd: rdd.toDF().registerTempTable("tag_counts"))
# Print the first ten elements of each RDD in this DStream
#words.pprint()
sqlContext = SQLContext(sc)
top = sqlContext.sql('select tag,count from tag_counts')
for row in top.collect():
    print(row.tag, row['count'])
print('----------------')

ssc.start()
ssc.awaitTermination()


from pyspark.streaming import StreamingContext
from pyspark import SparkContext

sc=SparkContext("local","pystreaming2")
ssc=StreamingContext(sc,1)
lines=ssc.socketTextStream("localhost",9999)
lines.pprint()

ssc.start()
ssc.awaitTermination()


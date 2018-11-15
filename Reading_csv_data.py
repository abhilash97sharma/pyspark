from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import translate
import pyspark.sql.functions as f
import re
def parseLine(line):
    s1 = re.sub('http.*', '', line)
    return s1
sc = SparkContext("local","Reading_csv_data")
sqlContext = SQLContext(sc)
file1=sqlContext.read.csv('file:///home/abhilash/Downloads/data.csv',header="true")
#file1.select("text", f.translate(f.col("text"), "RT@", "XXX").alias("replaced")).show()
#file1.select("sentiment","text",f.regexp_replace(f.col("text"),"[RT@|https*]","").alias("replaced"),"user").write.csv('file:///home/abhilash/Downloads/datamod.csv')
#fl=file1.filter(f.col("text").expr('%RT%'))
#fl.show(10,truncate=False)
rdd1=file1.select('text').rdd
rdd2=rdd1.map(parseLine)
for i in rdd2.collect():
     print(i)
#file1.show()

#working in the form of rdds.....
#file1.show(10,truncate=False)



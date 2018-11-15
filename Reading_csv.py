from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sc = SparkContext("local","Reading_csv")
schema1=StructType([StructField("make",StringType(),True),StructField("fueltype",StringType(),True),StructField("numofdoors",StringType(),True),StructField("bodystyle",StringType(),True),StructField("drivewheels",StringType(),True),StructField("enginelocation",StringType(),True),StructField("wheelbase",FloatType(),True),StructField("length",FloatType(),True),StructField("width",FloatType(),True),StructField("height",FloatType(),True),StructField("curbweight",IntegerType(),True),StructField("enginesize",IntegerType(),True),StructField("horsepower",IntegerType(),True),StructField("peakrpm",IntegerType(),True),StructField("citympg",IntegerType(),True),StructField("highwaympg",IntegerType(),True),StructField("price",IntegerType(),True)])
sqlContext = SQLContext(sc)
file1=sqlContext.read.csv('/Abhilash/cars.csv',header="true",schema=schema1)
file2=file1.limit(8)
file3=file2.select('make','length','width','height')
file3.show()
file4=file3.groupBy('make').min('length')
file4.show()
file5=file3.groupBy('make').max('width')
file5.show()
file6=file5.join(file4,file4.make == file5.make,"inner")
file6.show()
#file4.printSchema()


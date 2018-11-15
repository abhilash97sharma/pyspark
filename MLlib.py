from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
#from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
#def transformed(row):
#    return Row(label=row["price"],features=Vectors.dense([row["length"],row["width"],row["height"],row["horsepower"]]))
sc = SparkContext("local","MLlib")
schema1=StructType([StructField("make",StringType(),True),StructField("fueltype",StringType(),True),StructField("numofdoors",StringType(),True),StructField("bodystyle",StringType(),True),StructField("drivewheels",StringType(),True),StructField("enginelocation",StringType(),True),StructField("wheelbase",FloatType(),True),StructField("length",FloatType(),True),StructField("width",FloatType(),True),StructField("height",FloatType(),True),StructField("curbweight",IntegerType(),True),StructField("enginesize",IntegerType(),True),StructField("horsepower",IntegerType(),True),StructField("peakrpm",IntegerType(),True),StructField("citympg",IntegerType(),True),StructField("highwaympg",IntegerType(),True),StructField("price",IntegerType(),True)])
sqlContext = SQLContext(sc)
file1=sqlContext.read.csv('file:///home/abhilash/Documents/files/CSV_files/cars.csv',header="true",schema=schema1)
file1=file1.na.drop()
file2=file1.select('length','width','height','horsepower','price').limit(5)
#transformed = transformed(file2)
#file2.show(5)
#file2.describe().show(5,truncate=False)
#print(file2.columns) #working fine printing the names of the dataset columns.
fet_col=['length','width','height','horsepower']
vec_fea = VectorAssembler(inputCols=fet_col, outputCol='features')
#file4=file2.select('price')
#print(type(file4))
#file4.show()
final_data = vec_fea.transform(file2)
f = final_data.select('features')
f.show(5,truncate=False)
#final_data.show(5,truncate=False)
#file2.show(5,truncate=False)

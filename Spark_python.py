from pyspark import SparkContext
sc= SparkContext("local","Spark_python")
arr=[1,2,3,45,5,6,8]
v1=sc.parallelize(arr)
v3 = v1.filter(lambda x: (x%2==0))
print(v3.collect())

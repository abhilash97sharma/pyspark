from pyspark import SparkContext
from pyspark import SQLContext
def funinc(a):
    return(a+1)
sc=SparkContext("local","python_function")
arr = [1,2,3,4,5,6]
out = sc.parallelize(arr)
map2=out.map(funinc)
print(map2.collect())

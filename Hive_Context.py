from pyspark import HiveContext
from pyspark import SparkContext
sc= SparkContext("local","Hive_Context")
hv= HiveContext(sc)
print("Objects of hive context and sparkContext created")
hv.sql("select * from usrdb.emp")

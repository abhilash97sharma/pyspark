from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import *
import pyspark.sql.functions as f

sc = SparkContext('local','Titanic')
sqlContext = SQLContext(sc)

def fun1(val):
    if val=='0':
        val='No'
    else:
        val='Yes'
    return val

def fun2(val):
    if val=='1':
       val='1st'
    elif val=='2':
       val='2nd'
    else:
       val='3rd'
    return val

def fun3(val):
    if val > 0 and val <=15:
       val='0-15'
    elif val > 15 and val <=35:
        val='16-35'
    elif val > 35 and val <=60:
        val='35-60'
    else:
         val='>60'  
    return val

def fun4(val):
    return val[0]

f1 = udf(fun1)
f2 = udf(fun2)
f3 = udf(fun3)
f4 = udf(fun4)

file1=sqlContext.read.csv('file:///home/abhilash/Downloads/titanic3.csv',header='true')
# data formatting.
file1=file1.withColumn('Survived',f1(file1['survived']))
file1=file1.withColumn('Pclass',f2(file1['pclass']))
file1=file1.withColumn('Age',file1['age'].cast('float'))
file1=file1.withColumn('Category',f3(col('age')))
#file1.show(5)
#file1.printSchema()

#Analysis 1: For showing how many male and female people died.
#file12=file1.groupBy('sex','Survived').count()
#file12.show()

#Analysis 2: For showing the no of peoples are travelling in which class are survived or not
#file21=file1.groupBy('Pclass','sex','Survived').count()
#file21.show()

#Analysis 3: for showing the count of people survived or not of different age groups.
#file31=file1.groupBy('category','Survived').count()
#file31.show()

#Analysis 4: No of people survived or not in different cabins
#file41=file1.groupBy('cabin','Survived').count()
#file41.show()

#Analysis 5: showing the count at which station people started their journey
#file51=file1.groupBy('embarked','Survived').count()
#file51.show()

#Analysis 6:
#file61=file1.withColumn('Initial_char',f4(f.col('name')))
#file61.show(20,truncate=False)
#file62=file61.groupBy('Initial_char','Survived').count()
#file62.show(50,truncate=False)

#Analysis 7:
file71=file1.select('Survived','Pclass','Age','sex','embarked','cabin','parch','sibsp')
#print(file71.count())

#.........StringIndexer

file71=file71.na.drop()
indexer = StringIndexer(inputCol='Survived',outputCol='Survived_index')
indexed = indexer.fit(file71).transform(file71)
indexer = StringIndexer(inputCol='sex',outputCol='Sex_index')
indexed = indexer.fit(indexed).transform(indexed)
indexer = StringIndexer(inputCol='Pclass',outputCol='Pclass_index')
indexed = indexer.fit(indexed).transform(indexed)
indexer = StringIndexer(inputCol='embarked',outputCol='embarked_index')
indexed = indexer.fit(indexed).transform(indexed)

#......vector Assembler

#indexed.show()
#indexed.printSchema()
assembler = VectorAssembler(inputCols=['Survived_index','Sex_index','Pclass_index','embarked_index'],outputCol='features')
indexed = assembler.transform(indexed)
indexed.show(30,truncate=False)
#print(file71.count())
#file71.show()



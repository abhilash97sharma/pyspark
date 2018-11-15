#spark-submit --master yarn --deploy-mode client --driver-memory 512m --executor-memory 512m --num-executors 3 --executor-cores 2 /home/abhilash/Documents/pysparkcodes/Titanic_Ml.py
from pyspark.ml.feature import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark.sql.functions as f 

sc = SparkContext('local','Logistic_Regression')
sqlContext = SQLContext(sc)

titanic = sqlContext.read.csv('file:///home/abhilash/Downloads/titanic3.csv', header = 'true')
titanic = titanic.filter(f.col('sex').isNotNull())
titanic = titanic.withColumnRenamed('home.dest','homedest')

#titanic.groupBy('pclass').count().show()  #for checking any null value was present in the pclass attribute

#---------------------- for converting a sex column in the encoded form
indexer = StringIndexer(inputCol='sex',outputCol='sex_index')
indexed = indexer.fit(titanic).transform(titanic)
titanic = indexed

#------------------------ for converting a pclass column in the encoding form
indexer = StringIndexer(inputCol='pclass',outputCol='pclass_index')
indexed = indexer.fit(titanic).transform(titanic)
titanic = indexed

titanic = titanic.filter(f.col('embarked').isNotNull()) # removing a null values entries

#------------------------- for converting a embarked column in the encoding form
indexer = StringIndexer(inputCol='embarked',outputCol='embarked_index')
indexed = indexer.fit(titanic).transform(titanic)
titanic = indexed

#--------------------------- for converting sibsp column in the encoding form
indexer = StringIndexer(inputCol='sibsp',outputCol='sibsp_index')
indexed = indexer.fit(titanic).transform(titanic)
titanic = indexed

#--------------------------- for converting parch column in the encoding form
indexer = StringIndexer(inputCol='parch',outputCol='parch_index')
indexed = indexer.fit(titanic).transform(titanic)
titanic = indexed

#----------------------------- for converting a survived column in the encoding form
indexer = StringIndexer(inputCol='survived',outputCol='label')
indexed = indexer.fit(titanic).transform(titanic)
titanic = indexed

#------------------------------- for converting a columns into a feature vector
vector = VectorAssembler(inputCols=['sex_index','pclass_index','embarked_index','sibsp_index','parch_index'],outputCol='features')
titanic = vector.transform(titanic)

#---------------------------- for extracting a features and label column
output = titanic.select('features','label')
output.show(6,truncate=False)

#titanic.groupBy('embarked').count().show()   # for checking any null values was present in the embarked column

#titanic.groupBy('sex').count().show(4,truncate=False)  #for checking the wheather the null was present in it.
#titanic.printSchema()
#titanic.show(5,truncate=False)

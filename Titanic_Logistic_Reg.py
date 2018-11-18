from pyspark.ml.feature import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
import pyspark.sql.functions as f 

sc = SparkContext('local','Logistic_Regression')
sqlContext = SQLContext(sc)

titanic = sqlContext.read.csv('file:///home/abhilash/Downloads/titanic3.csv', header = 'true')
titanic = titanic.filter(f.col('sex').isNotNull())
titanic = titanic.filter(f.col('embarked').isNotNull())

titanic = titanic.withColumnRenamed('home.dest','homedest')

categoricalColumns = ['sex', 'pclass', 'embarked', 'sibsp', 'parch']
stages = []

for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]

label_stringIdx = StringIndexer(inputCol = 'survived', outputCol = 'label')
stages += [label_stringIdx]

assemblerInputs = [c + "classVec" for c in categoricalColumns]

assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

pipeline = Pipeline(stages = stages)
pipelineModel = pipeline.fit(titanic)
df = pipelineModel.transform(titanic)
selectedCols = ['label', 'features']
df = df.select(selectedCols)

#dividing the dataset into training and testing.
train, test = df.randomSplit([0.7, 0.3], seed = 2018)
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
lrModel = lr.fit(train)

#making a predictions
predictions = lrModel.transform(test)
predictions.show(350,truncate=False)

#print(lrModel.coefficients)


#df.show(5,truncate=False)
#df.printSchema()



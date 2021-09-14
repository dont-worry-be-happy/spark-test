from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder.getOrCreate()

df = spark.read.options(inferSchema='True') \
    .csv('/iris-data.csv') \
    .toDF("sepal_length", "sepal_width", "petal_length", "petal_width", "class")

# setup for data preparation
indexer = StringIndexer(inputCol="class", outputCol="label")
va = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol="features")
indexer_labels = indexer.fit(df).labels
# setup regression
lr = LogisticRegression(regParam=1e-6, fitIntercept=True, maxIter=100, tol=0.0001)
# prepare data and build model
lr_pipeline = Pipeline(stages=[indexer, va, lr])
model = lr_pipeline.fit(df)

# prediction input
pred_data = spark.createDataFrame(
    [(5.1, 3.5, 1.4, 0.2),
     (6.2, 3.4, 5.4, 2.3)],
    ["sepal_length", "sepal_width", "petal_length", "petal_width"])

# predict
prediction = model.transform(pred_data)

# restore string names
restore = IndexToString(inputCol="prediction", outputCol="class", labels=indexer_labels)
result = restore.transform(prediction)[['class']]

# save result
result.toPandas().to_csv('../out/out_3_2.txt', index=False)

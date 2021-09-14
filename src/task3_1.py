# note: upload through sparkContext is not scalable but is fine for smaller files like this
from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.addFile('https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data')
df = spark.read.csv('file:' + SparkFiles.get('iris.data'))
# save in hdfs/dbfs for future use
df.write.csv('/iris-data.csv', header=False)

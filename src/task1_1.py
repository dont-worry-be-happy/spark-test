# note: upload through sparkContext is not scalable but is fine for smaller files like this
from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.addFile('https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv')
rdd = spark.sparkContext.textFile('file:' + SparkFiles.get('groceries.csv'))
# save in hdfs/dbfs for future use
rdd.saveAsTextFile('/groceries.csv')


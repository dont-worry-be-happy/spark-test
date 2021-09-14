# note: upload through sparkContext is not scalable but is fine for smaller files like this
from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.addFile('https://github.com/databricks/LearningSparkV2/raw/master/mlflow-project-example/data/sf-airbnb-clean.parquet/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet')
df = spark.read.parquet('file:' + SparkFiles.get('part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet'))
# save in hdfs/dbfs for future use
df.write.parquet('/sf-airbnb-clean.parquet')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

df = spark.read.parquet('/sf-airbnb-clean.parquet')

res1 = df.agg(F.min(df.price).alias("min_price"),
              F.max(df.price).alias('max_price'),
              F.count('*').alias('row_count'))
# results are collected through driver memory, it is assumed to be ok for the purpose of this exercise
res1.toPandas().to_csv('../out/out_2_2.txt', index=False)


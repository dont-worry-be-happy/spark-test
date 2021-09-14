from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

df = spark.read.parquet('/sf-airbnb-clean.parquet')

res2 = df.filter((df.price > 5000) & (df.review_scores_value == 10.0)).agg(
    F.avg(df.bathrooms).alias('avg_bathrooms'),
    F.avg(df.bedrooms).alias('avg_bedrooms'))
# results are collected through driver memory, it is assumed to be ok for the purpose of this exercise
res2.toPandas().to_csv('../out/out_2_3.txt', index=False)


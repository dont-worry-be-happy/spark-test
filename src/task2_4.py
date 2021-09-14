from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

df = spark.read.parquet('/sf-airbnb-clean.parquet')


# filter for lowest price
cheapest = df[['price', 'accommodates', 'review_scores_value']].join(
    df.agg(F.min('price').alias('price')), on='price', how='leftsemi')
# filter highest review from cheapest
best_rev = cheapest.join(
    cheapest.agg(F.max('review_scores_value').alias('review_scores_value')),
    on='review_scores_value', how='leftsemi')

# results are collected through driver memory, it is assumed to be ok for the purpose of this exercise
# select max accommodates in case there is more than one cheapest with best review
max_acc = best_rev.select(F.max('accommodates').alias('max')).limit(1).collect()[0].max
with open('../out/out_2_4.txt', 'w') as f:
    f.write(f'{max_acc}\n')

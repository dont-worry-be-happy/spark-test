from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
products = spark.sparkContext.textFile("/groceries.csv").flatMap(lambda line: line.split(","))
dist_prod = products.distinct()

# results are collected through driver memory, it is assumed to be ok for the purpose of this exercise
with open('../out/out_1_2a.txt', 'w') as f:
    f.writelines(f"{l}\n" for l in dist_prod.collect())

# from task description it is not clear if count should be:
# a) total number of products listed in all transactions
# b) number of unique products
# option a) was chosen for implementation:
with open('../out/out_1_2b.txt', 'w') as f:
    f.write(f"Count:\n{products.count()}\n")




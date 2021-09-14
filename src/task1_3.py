from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
products = sc.textFile("/groceries.csv").flatMap(lambda line: line.split(","))
prod_cnt = products.map(lambda p: (p, 1)).reduceByKey(lambda x, y: x + y)

# results are collected through driver memory, it is assumed to be ok for the purpose of this exercise
with open('../out/out_1_2c.txt', 'w') as f:
    for line in prod_cnt.takeOrdered(5, key=lambda x: -x[1]):
        f.write(f"({line[0]}:{line[1]})\n")

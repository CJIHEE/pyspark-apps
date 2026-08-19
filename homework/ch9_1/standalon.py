from pyspark.ml.torch import data
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName('standalone')  \
    .getOrCreate()

schema = 'id TNT, country STRING, hit LONG'
df = spark.createDataFrame(data=[(1,'Korea',120),(2,'USA',80),(3,'Japan',40)], schema=schema)
print(df.count())

#sleep 10 minute
time.sleep(600)
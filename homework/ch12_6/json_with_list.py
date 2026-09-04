from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col

schema = StructType([
    StructField('name',StringType(),False),
    StructField('address',StringType()
            [
                StructField('country',StringType(),False),
                StructField('city',StringType(),False),
            ]
    ,False),
    StructField('age',IntegerType(),False),
    StructField('hobbies',ArrayType(StringType(),True),True)
])

def for_each_batch_function(df :DataFrame, epoch_id):
    print(f'====================================== epoch_id: {epoch_id} start ======================================')
    df.persist()

    df.show(truncate=False)

    from_json_df=df.select(
        from_json(col('VALUE'),schema).alias('person_info')
    )
    from_json_df.printSchema()
    json_schema = from_json_df.schema
    print(json_schema)


app_name = 'from_json_test'
spark = SparkSession \
         .builder \
         .appName(app_name) \
         .getOrCreate()

kafka_source_df = spark.readStream \
                  .format("kafka") \
                  .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
                  .option("subscribe", "lesson.spark-streaming.person_info") \
                  .option('startingOffsets','latest') \
                  .option('failOndataLoss','false') \
                  .load \
                  .selectExpr(
                    "CAST(key As STRING) AS KEY"
                    "CAST(value AS STRING AS VALUE"
                  )

query = kafka_source_df.writeSteam \
        .foreachBatch(for_each_batch_function) \
        .option("checkpointLocation", f'/home/spark/kafka_offsets/{app_name}') \
        .start()

query.awaitTermination()
def main(self):
    spark = self.get_session_builder().getOrCreate()

    spark.sparkContext.setCheckpointDir(self.dataframe_chkpnt_dir)

    self.init_call(spark)

    key_schema = StructType([
        StructField('STT_ID', StringType()),
        StructField('CRT_DTTM', StringType())
    ])

    value_schema = StructType([
        StructField('STT_NM', StringType()),
        StructField('TOT_RACK_CNT', IntegerType()),
        StructField('TOT_PRK_CNT', IntegerType()),
        StructField('STT_LTTD', StringType()),
        StructField('STT_LGTD', StringType())
    ])

    streaming_query = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
        .option("subscribe", "apis.seouldata.rt-bicycle") \
        .option('failOnDataLoss', 'false') \
        .option('startingOffsets', 'earliest') \
        .option('maxOffsetsPerTrigger', '10000') \
        .load() \
        .selectExpr(
            "CAST(key AS STRING) AS KEY",
            "CAST(value AS STRING) AS VALUE"
        ) \
        .select(
            from_json(col('KEY'), key_schema).alias('KEY'),
            from_json(col('VALUE'), value_schema).alias('VALUE')
        ) \
        .select(
            col('KEY.STT_ID').alias('stt_id'),
            col('KEY.CRT_DTTM').alias('crt_dttm'),
            col('VALUE.STT_NM').alias('stt_nm'),
            col('VALUE.TOT_RACK_CNT').alias('tot_rack_cnt'),
            col('VALUE.TOT_PRK_CNT').alias('tot_prk_cnt'),
            col('VALUE.STT_LTTD').alias('stt_lttd'),
            col('VALUE.STT_LGTD').alias('stt_lgtd')
        ) \
        .writeStream \
        .foreachBatch(lambda df, epoch: self.for_each_batch(df, epoch, spark)) \
        .option("checkpointLocation", self.kafka_offset_dir) \
        .start()

    streaming_query.awaitTermination()
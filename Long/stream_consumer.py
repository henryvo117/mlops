from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType


if __name__ == '__main__':
    KAFKA_TOPIC = "numtest"
    KAFKA_SERVER = "localhost:9092"
    schema = StructType([
        StructField("date", TimestampType(), False),
        StructField("id", IntegerType(), False),
        StructField("time", FloatType(), False),
    ])
    # creating an instance of SparkSession
    spark_session = SparkSession \
        .builder \
        .appName("Python Spark create RDD") \
        .getOrCreate()

    # Subscribe to 1 topic
    df = spark_session \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").\
        withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")). \
        withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))
    parsedDF = df.select(from_json(col("value"), schema).alias('value'))
    flattenDF = parsedDF.selectExpr("value.date", "value.id", "value.time")
    flattenDF.printSchema()
    repo_path = 'output_data'
    flattenDF.writeStream \
        .format("csv")\
        .trigger(processingTime="10 seconds")\
        .option("format", "append")\
        .option("checkpointLocation", "/tmp/bruv/checkpoint_6")\
        .option("path", repo_path)\
        .outputMode("append")\
        .start()\
        .awaitTermination()
    # df.writeStream.format("console").outputMode("append").start().awaitTermination()


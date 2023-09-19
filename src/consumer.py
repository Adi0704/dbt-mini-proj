from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("weatherData") \
    .getOrCreate()


schema = StructType().add("precip_rate", IntegerType()).add("rh", IntegerType()) \
    .add("wind_spd", FloatType()).add("snow_rate", IntegerType()).add("app_temp", FloatType()) \
    .add("pres", FloatType()).add("azimuth", FloatType()).add("dewpt", FloatType()).add("uv", FloatType()).add("elev_angle", FloatType()) \
    .add("wind_dir", IntegerType()).add("ghi", FloatType()).add("dhi", FloatType()) \
    .add("solar_rad", FloatType()).add("vis", IntegerType()).add("dni", FloatType()).add("temp", FloatType()).add("slp", FloatType()) \
    .add("clouds", IntegerType()).add("ts", IntegerType()).add("city", StringType())

incoming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","weather") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

raw_df = incoming_df.selectExpr("CAST(value AS STRING)")

s_df = raw_df.select(from_json(raw_df.value, schema).alias("data"))

weather_df = s_df.select(col("data.temp"),col("data.app_temp"),col("data.pres"),col("data.rh"),col("data.uv"),col("data.city"),col("data.ts"))

output_df = weather_df.select(to_json(struct(col("*"))).alias("value"))

query = output_df.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation", "/tmp/spark_checkpoint").option("topic", "output").outputMode("update").start()

query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, to_json, array, col, struct, udf
from operator import add
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

# Create SparkSession and configure it
spark = SparkSession.builder.appName('streamTest') \
    .config('spark.master','spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max',1) \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.streaming.checkpointLocation','hdfs://namenode:9000/stream-checkpoint/') \
    .getOrCreate()

# Create a read stream from Kafka and a topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("startingOffsets", "earliest")\
    .option("subscribe", "sentences") \
    .load()

# Cast to string
sentences = df.selectExpr("CAST(value AS STRING)")
result = 1
# Create a Kafka write stream, with the output mode "complete"
result.select(to_json(struct([result[x] for x in result.columns])).alias("value")).select("value")\
    .writeStream\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "sentiment-scores") \
    .outputMode("append") \
    .start().awaitTermination()
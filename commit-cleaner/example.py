from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, explode, split, to_json, array, col, struct, udf
from operator import add
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

@udf(returnType=StringType())
def processValue(element):
    return element

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
    .option("subscribe", "connecttest") \
    .load()

structureSchema = StructType().add("sha", StringType())
data_as_string = df.selectExpr("CAST(value AS STRING)")
data_as_json = data_as_string.select(from_json(col("value"), structureSchema).alias("data")).select("data.*")

data_as_json.select(to_json(struct(data_as_json[x] for x in data_as_json.columns)).alias("value")).select("value")\
        .writeStream\
        .format('kafka')\
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "sentiment-scores") \
        .outputMode("append") \
        .start().awaitTermination(100).stop()
# Cast to string
#[data_as_json[0] for x in data_as_json.columns]

# Create a Kafka write stream, with the output mode "complete"
#json_commit = commit.select(from_json(col("value.from_json(value)"), structureSchema)).alias("commit")#.select("commit")

#print(json_commit)

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, explode, split, to_json, array, col, struct, udf, lit
from operator import add
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

@udf(returnType=StringType())
def getRepoName(url):
    reposName = "repos"
    splitUrl = url.split("/")
    repoIndex = splitUrl.index(reposName)
    repoIndex+= 2
    repoCommit = splitUrl[repoIndex]
    return repoCommit

def createFileStructureSchema():
    return StructType()\
        .add("filename", StringType())\
        .add("additions", IntegerType())\
        .add("deletions", IntegerType())\
        .add("changes", IntegerType())\
        .add("status", StringType())

spark = SparkSession.builder.appName('streamTest') \
    .config('spark.master','spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max',1) \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.streaming.checkpointLocation','hdfs://namenode:9000/stream-checkpoint/') \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("startingOffsets", "earliest")\
    .option("subscribe", "connecttest")\
    .load()

structureSchema = StructType()\
    .add("commit", StructType().add("author", StructType().add("name", StringType()).add("email", StringType()).add("date", TimestampType())))\
    .add("url", StringType())\
    .add("sha", StringType())\
    .add("stats", StructType().add("additions", IntegerType()).add("deletions", IntegerType()).add("total", IntegerType()))\
    .add("files", ArrayType(createFileStructureSchema()))\

data_as_string = df.selectExpr("CAST(value AS STRING)")
data_as_json = data_as_string.select(from_json(col("value"), structureSchema).alias("data")).select("data.*")

data_as_json = data_as_json.withColumn("repo", getRepoName(data_as_json.url))
data_as_json = data_as_json.select(col("commit.author").alias("commit_author"), col("stats"), col("files"), col("repo"), col("sha").alias("id"))

data_as_json.select(to_json(struct([data_as_json[x] for x in data_as_json.columns])).alias("value")).select("value")\
    .writeStream\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "clean-commits") \
    .outputMode("append") \
    .start().awaitTermination()

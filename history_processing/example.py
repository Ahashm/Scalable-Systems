from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import sum, explode, to_json, array, col, struct, udf, lit, collect_list
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

my_spark = SparkSession \
    .builder \
    .appName("myAppName") \
    .master("spark://spark-master:7077")\
    .config("spark.mongodb.input.uri", "mongodb://root:rootpassword@mongodb_container:27017") \
    .config("spark.mongodb.output.uri", "mongodb://root:rootpassword@mongodb_container:27017") \
    .config("spark.mongodb.connection.uri", "mongodb://root:rootpassword@mongodb_container:27017") \
    .config("spark.mongodb.database", "spotify") \
    .config("spark.mongodb.collection", "commits") \
    .config('spark.executor.cores', "1")\
    .config('spark.cores.max',"1")\
    .config("spark.executor.memory", "1g")\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0")\
    .getOrCreate()
    
@udf(returnType=IntegerType())
def totalAdditions(file):
    addedCodeLines = file.additions - file.deletions
    return addedCodeLines

@udf(returnType=StringType())
def getFileType(fileName):
    addedCodeLines = fileName.split(".")
    return addedCodeLines[-1]

files = my_spark.read.format("mongodb").load()

files.printSchema()

files.show()

per_file = files.withColumn("file", explode(files.files))
per_file = per_file.select(col("repo"), col("commit_author"), col("file"))
per_file = per_file.withColumn("added_lines", totalAdditions(per_file.file))
per_file = per_file.withColumn("filetype", getFileType(per_file.file.fileName))
per_file = per_file.groupBy("repo", "filetype").agg(sum("added_lines").alias("added_lines"))
per_file = per_file.groupBy("repo").agg(sum("added_lines").alias("total_lines"),collect_list(struct("filetype", "added_lines")).alias("filetypes"))

per_file.write.format("mongodb").mode("overwrite").option("database",
"spotify").option("collection", "overview").save()



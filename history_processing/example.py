from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, explode, split, to_json, array, col, struct, udf, lit
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
per_file = per_file.withColumn("filename", per_file.file.filename)
per_file = per_file.select(col("repo"), col("commit_author"), col("file"), col("filename"))
per_file = per_file.withColumn("added_lines", totalAdditions(per_file.file))
per_file = per_file.withColumn("file_type", getFileType(per_file.fileName))

per_file.printSchema()
per_file.show()

total_collection = per_file.reduce
# Read in all files in the directory
#txtFiles = sc.wholeTextFiles(files, 20)
# Take the content of the files and split them
#all_word = txtFiles.flatMap(lambda s: s[1].split())
# Change from list of words to list of (word, 1)
#word_map = all_word.map(lambda s: (s, 1))
# Merge values with equal keys
#word_reduce = word_map.reduceByKey(lambda s, t: s+t)
# Filter using the defined lambda and sort by value
#top_words = word_reduce.filter(select_words).sortBy(lambda s: s[1])
# Save as text file
#top_words.saveAsTextFile('hdfs://namenode:9000/txt-out')
# Collect to a Python list and print
#print(top_words.collect())


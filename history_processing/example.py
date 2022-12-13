from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import sum, explode, from_json, collect_set, col, struct, udf, collect_list, to_timestamp, hour, count, first, current_timestamp, add_months
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
    isString = isinstance(fileName, str)
    if(not isString):
        fileName = fileName[0]
    addedCodeLines = fileName.split(".")
    return addedCodeLines[-1]

file_structure = StructType()\
        .add("filename", StringType())\
        .add("additions", IntegerType())\
        .add("deletions", IntegerType())\
        .add("changes", IntegerType())\
        .add("status", StringType())

def getFilesOverview(dataframe):
    per_file = dataframe.withColumn("file", explode(files.files))
    per_file = per_file.select(col("repo"), col("commit_author"), col("file").alias("filestring"))
    per_file = per_file.withColumn("file", from_json(col("filestring"), file_structure))
    per_file = per_file.withColumn("added_lines", totalAdditions(per_file.file))
    per_file = per_file.withColumn("filetype", getFileType(per_file.file.filename))
    per_file = per_file.groupBy("repo", "filetype").agg(sum("added_lines").alias("code_lines"))
    per_file = per_file.groupBy("repo").agg(sum("code_lines").alias("total_lines"),collect_list(struct("filetype", "code_lines")).alias("filetypes"))
    return per_file

def getActiveWorkingHours(files):
    active_working_hours = files.select(col("repo"), col("commit_author.date").alias("date"))
    active_working_hours = active_working_hours.withColumn("commit_hour", hour(to_timestamp("date")))
    active_working_hours = active_working_hours\
        .withColumn("count", count("commit_hour").over(window))
    active_working_hours = active_working_hours.orderBy("count", ascending = False)
    active_working_hours = active_working_hours.groupBy("repo").agg(first("commit_hour").alias("commit_hour"))
    return active_working_hours
    

window = Window.partitionBy("repo", "commit_hour")
files = my_spark.read.format("mongodb").load()

files.printSchema()

repo_overview = getFilesOverview(files)
active_working_hours = getActiveWorkingHours(files)
repo_overview = repo_overview.join(active_working_hours, "repo", "inner")

recent_contributors = files\
    .filter(add_months(to_timestamp(current_timestamp()), 1) > to_timestamp("commit_author.date"))\
    .groupBy("repo").agg(collect_set("commit_author.name").alias("committers")).select(col("repo"), col("committers"))
recent_contributors.printSchema()
repo_overview = repo_overview.join(recent_contributors, "repo", "left_outer")


repo_overview.write.format("mongodb").mode("overwrite").option("database",
"spotify").option("collection", "overview").save()



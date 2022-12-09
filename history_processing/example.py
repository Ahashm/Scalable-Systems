from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
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
    
# Filter function
select_words = lambda s : s[1] > 400

files = my_spark.read.format("mongodb").load()

files.printSchema()
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


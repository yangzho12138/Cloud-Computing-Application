from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

sc = SparkContext()
sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName("mp8").getOrCreate()

####
# 1. Setup (10 points): Download the gbook file and write a function to load it in an RDD & DataFrame
####

rdd = sc.textFile("gbooks")

# RDD API
# Columns:
# 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)

schema = StructType([
    StructField("word", StringType(), True),
    StructField("count1", IntegerType(), True),
    StructField("count2", IntegerType(), True),
    StructField("count3", IntegerType(), True),
])

# Spark SQL - DataFrame API
df = spark.createDataFrame(rdd.map(lambda x : [y if i == 0 else int(y) for i, y in enumerate(x.split('\t'))]), schema)

####
# 4. MapReduce (10 points): List the three most frequent 'word' with their count of appearances
####

# Spark SQL

df.createOrReplaceTempView("gbooks")
spark.sql("SELECT word, COUNT(*) FROM gbooks GROUP BY word ORDER BY COUNT(*) DESC").show(3)

# There are 18 items with count = 425, so could be different 
# +---------+--------+
# |     word|count(1)|
# +---------+--------+
# |  all_DET|     425|
# | are_VERB|     425|
# |about_ADP|     425|
# +---------+--------+
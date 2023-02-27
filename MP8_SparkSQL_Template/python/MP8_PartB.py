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

df = spark.createDataFrame(rdd.map(lambda x : [y if i == 0 else int(y) for i, y in enumerate(x.split('\t'))]), schema)

####
# 2. Counting (10 points): How many lines does the file contains? Answer this question via both RDD api & #Spark SQL
####

# RDD api
# df.count()

# Spark SQL 
df.createOrReplaceTempView("gbooks")
spark.sql("SELECT COUNT(*) FROM gbooks").show()

# +--------+                                                                              
# |count(1)|
# +--------+
# |86618505|
# +--------+
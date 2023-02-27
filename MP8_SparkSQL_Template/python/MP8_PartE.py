from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

sc = SparkContext()
sqlContext = SQLContext(sc)
sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

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
# 5. Joining (10 points): The following program construct a new dataframe out of 'df' with a much smaller size.
####

df2 = df.select("word", "count1").distinct().limit(100)
df2.createOrReplaceTempView('gbooks2')

# Now we are going to perform a JOIN operation on 'df2'. Do a self-join on 'df2' in lines with the same #'count1' values and see how many lines this JOIN could produce. Answer this question via DataFrame API and #Spark SQL API
# Spark SQL API

res = spark.sql("SELECT * FROM gbooks2 AS g1 JOIN gbooks2 AS g2 ON g1.count1 = g2.count1")
print(res.count())


# output: 218
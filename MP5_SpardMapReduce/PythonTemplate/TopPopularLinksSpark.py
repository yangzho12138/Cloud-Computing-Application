#!/usr/bin/env python

# spark-submit TopPopularLinksSpark.py dataset/links/ partD

import sys
from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

# get link numbers for all pages
rdd_flatmap = lines.flatMap(lambda line : [page for page in re.split(" ", line.split(":")[1].strip())])
rdd_map = rdd_flatmap.map(lambda page : (page, 1))
pageRank = rdd_map.reduceByKey(lambda agg, curr: agg + curr).sortBy(lambda p : p[1], ascending=False).take(10)
pageRank = sorted(pageRank, key=lambda p : p[0])

output = open(sys.argv[2], "w")

#write results to output file. Foramt for each line: (key + \t + value +"\n")
for p in pageRank:
    output.write(p[0] + '\t' + str(p[1]) + '\n')

sc.stop()


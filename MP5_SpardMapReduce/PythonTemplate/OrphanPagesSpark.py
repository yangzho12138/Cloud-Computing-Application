#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

# init all the pages
rdd_pages = lines.flatMap(lambda line : [page for page in re.split(" |:", line) if page != ''])
rdd_map = rdd_pages.map(lambda page : (page, 0))

# find the pages that are not orphanpage
rdd_flatmap = lines.flatMap(lambda line : [page for page in re.split(" ", line.split(":")[1].strip()) if page != line.split(":")[0].strip() or len(re.split(" ", line.split(":")[1].strip())) != 1])
rdd_map = rdd_map.union(rdd_flatmap.map(lambda page : (page, 1)))

orphanPages = rdd_map.reduceByKey(lambda agg, curr: agg + curr).filter(lambda page: page[1] == 0).collect()
pageRank = sorted(orphanPages, key=lambda p : p[0])

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line + "\n")
for p in pageRank:
    output.write(p[0] + '\n')

sc.stop()


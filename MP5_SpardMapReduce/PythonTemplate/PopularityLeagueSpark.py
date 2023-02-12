#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

rdd_flatmap = lines.flatMap(lambda line : [page for page in re.split(" ", line.split(":")[1].strip())])
rdd_map = rdd_flatmap.map(lambda page : (page, 1))

leagueIds = sc.textFile(sys.argv[2], 1)

rdd_leagues = leagueIds.flatMap(lambda league : league.split(" ")).collect()

pageRank = rdd_map.reduceByKey(lambda agg, curr: agg + curr).filter(lambda page : page[0] in rdd_leagues).collect()
pageRank = sorted(pageRank, key=lambda p : p[1])
print(pageRank)

# rank the leagues
ranks = list()

rank = -1
accRank = 0
lastPageLinks = -1

for p in pageRank:
    if p[1] == lastPageLinks:
        rank += 1
        accRank += 1
    else:
        lastPageLinks = p[1]
        rank += 1
        accRank = 0
    ranks.append((p[0], rank - accRank))

ranks = sorted(ranks, key=lambda r : r[0])

output = open(sys.argv[3], "w")

#write results to output file. Foramt for each line: (key + \t + value +"\n")
for r in ranks:
    output.write(r[0] + '\t' + str(r[1]) + '\n')

sc.stop()


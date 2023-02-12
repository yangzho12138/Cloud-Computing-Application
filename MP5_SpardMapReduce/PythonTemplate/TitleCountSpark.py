#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext
import re

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

stopWords = []

with open(stopWordsPath) as f:
    for word in f:
        stopWords.append(word[:-1])

with open(delimitersPath) as f:
    for characters in f:
        delimiters = characters

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)
delimiters = delimiters.strip()
delimiterString = ""
for d in delimiters:
    if d == '+' or '*' or '/': # key words in re
        delimiterString += '[' + d + ']|'
    else:
        delimiterString += d + "|"
delimiterString = delimiterString[:len(delimiterString)-1]

rdd_flatmap = lines.flatMap(lambda line : [w.lower() for w in re.split(delimiterString, line) if w.lower() not in stopWords and w != ""])

rdd_map = rdd_flatmap.map(lambda word : (word, 1))
topTitle = rdd_map.reduceByKey(lambda agg, curr: agg + curr).sortBy(lambda count: count[1], ascending=False).take(10)
topTitle = sorted(topTitle, key=lambda t:t[0])

outputFile = open(sys.argv[4],"w")

#TODO
#write results to output file. Foramt for each line: (line +"\n")
for t in topTitle:
    outputFile.write(t[0] + '\t' + str(t[1]) + '\n')

sc.stop()

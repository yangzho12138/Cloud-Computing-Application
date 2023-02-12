#!/usr/bin/env python

'''Exectuion Command: spark-submit TopTitleStatisticsSpark.py partA partB'''

import sys
from pyspark import SparkConf, SparkContext
import math

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

rating_data = lines.flatMap(lambda line : [int(num) for num in line.split('\t') if num[0] >= '0' and num[0] <= '9'])

data = rating_data.collect()
data = sc.parallelize(data)

mean = math.floor(data.mean())
sum = data.sum()
min = data.min()
max = data.max()
var = math.floor(data.variance())



outputFile = open(sys.argv[2], "w")


# write results to output file. Format
outputFile.write('Mean\t%s\n' % mean)
outputFile.write('Sum\t%s\n' % sum)
outputFile.write('Min\t%s\n' % min)
outputFile.write('Max\t%s\n' % max)
outputFile.write('Var\t%s\n' % var)

sc.stop()


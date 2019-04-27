from __future__ import print_function
from pyspark import SparkContext, SparkConf
import sys

sc = SparkContext()

with open('text.txt', 'r') as f:
	docsize = f.read().splitlines()

file = sc.textFile('text.txt')
rdd = file.flatMap(lambda x:x.split(' '))
rdd = rdd.map(lambda x: (x,1))
rdd = rdd.reduceByKey(lambda x,y: x+y)
rdd = rdd.map(lambda x:(x[1],x[0]))

for x in range(len(docsize)):
	print("\tDoc",x+1, end='')

print('\n')
#rdd = file.filter(lambda x:x!=' ').map(lambda word:(word.lower(),1).split()).reduceByKey(lambda x,y:x+y)
for xs in rdd.values().take(7):
	print(xs)

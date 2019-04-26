from pyspark import SparkContext, SparkConf
import sys

sc = SparkContext()

file = sc.textFile('text.txt')
rdd = file.flatMap(lambda x:x.split())
rdd = rdd.map(lambda x: (x,1))
rdd = rdd.reduceByKey(lambda x,y: x+y)
rdd = rdd.map(lambda x:(x[1],x[0]))
#rdd = file.filter(lambda x:x!=' ').map(lambda word:(word.lower(),1).split()).reduceByKey(lambda x,y:x+y)
for xs in rdd.take(5):
	print(xs)


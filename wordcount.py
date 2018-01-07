from pyspark import SparkContext

sc = SparkContext()
rdd = sc.textFile("iliad.mb.txt")
result = rdd.flatMap(lambda sentence: sentence.split())\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda v1, v2: v1 + v2)\
    .sortBy(lambda wc: -wc[1])\
    .take(10)

print(result)
#!/usr/bin/env python3

# 导入包
from pyspark import SparkContext
# 输入输出路径，输出路径不需要自己创建，系统会自动生成
inputFile = 'hdfs://localhost:9000/wordcount/word.txt'
outputFile = 'hdfs://localhost:9000/wordcount-out/spark-out'

sc = SparkContext('local', 'wordcount')
text_file = sc.textFile(inputFile)

counts = text_file.flatMap(lambda line: line.split(' ')).map(
    lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
counts.saveAsTextFile(outputFile)

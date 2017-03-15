from pyspark import SparkContext,SparkConf

sc = SparkContext(SparkConf())

row_data = sc.textFile('hdfs://master:9000/gmc/hour.csv')
print()
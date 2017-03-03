from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName='Streaming')
ssc = StreamingContext(sc,1)

lines = ssc.socketTextStream('localhost',9999)

words = lines.flatMap(lambda x:x.split(' '))

pairs = words.map(lambda x: (x,1))
wordCount = pairs.reduceByKey(lambda x,y :x+y)

wordCount.pprint()

ssc.start()
ssc.awaitTermination()

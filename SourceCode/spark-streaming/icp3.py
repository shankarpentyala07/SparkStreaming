import os

os.environ["SPARK_HOME"] = "C:\\shankar\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"



from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def reduceword(value1,value2):
    listmerge = value1 + value2
    setmerge = set(listmerge)
    listres=[]
    for x in setmerge:
        listres.append(x)
    return listres

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 30)
# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)
# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
# Count each word in each batch
pairs = words.map(lambda word: (len(word), [word]))

wordCounts = pairs.reduceByKey(reduceword)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
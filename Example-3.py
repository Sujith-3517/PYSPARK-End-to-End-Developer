from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext("local[2]", "SparkStreamingExample")

ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 9999)

words = lines.flatMap(lambda line: line.split(" "))

word_count_tuples = words.map(lambda word: (word, 1))

word_counts = word_count_tuples.reduceByKey(lambda x, y: x + y)

word_counts.pprint()

# Start the StreamingContext
ssc.start()

# Await termination of the StreamingContext
ssc.awaitTermination()

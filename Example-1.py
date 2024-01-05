from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("WordCountApp")

sc = SparkContext(conf=conf)

text_file_path = 'path/to/your/text/file'

text_rdd = sc.textFile(text_file_path)

words = text_rdd.flatMap(lambda line: line.split(" "))

word_count_tuples = words.map(lambda word: (word, 1))

word_counts = word_count_tuples.reduceByKey(lambda x, y: x + y)

result = word_counts.collect()

for (word, count) in result:
    print(f"{word}: {count}")

sc.stop()

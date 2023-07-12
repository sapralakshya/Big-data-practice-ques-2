from pyspark import SparkContext

# Create a Spark context
sc = SparkContext("local", "WordCount")

# Read the text file and split it into words
lines = sc.textFile(r"C:\Users\jayes\OneDrive\Desktop\DSA\sample.txt")
words = lines.flatMap(lambda line: line.split(" "))

# Map each word to a tuple (word, 1) and then reduce by key to get word counts
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Print the word counts
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Stop the Spark context
sc.stop()

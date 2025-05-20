from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder.master('local').appName('les').getOrCreate()

lines = spark.readStream.format('socket').option('host', 'localhost').option('port', 19999).load()

# splits in woorden
words = lines.select(explode(split(lines.value, ' ')).alias('word'))
# genereer aantal
wordCounts = words.groupby('word').count()

# output
query = wordCounts.writeStream.outputMode('update').format('console').start()
# forEachBatch
query.awaitTermination()

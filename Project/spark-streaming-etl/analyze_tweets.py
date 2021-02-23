from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession

from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import lower, col, regexp_replace

spark = SparkSession \
    .builder \
    .appName("TweetAndStockApp") \
    .getOrCreate()

# Get stock tickers
ticker_df = spark \
    .read \
    .load("NASDAQ_tickers.csv",
        format="csv", inferSchema="true", header="true")

ticker_df = ticker_df.select(lower(col('Symbol')).alias('Symbol'))
ticker_df.show()

# Get tweet stream from kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:9092") \
  .option("subscribe", "TWEETS") \
  .option("startingOffsets", "earliest") \
  .load()

# Split the lines into words
words = df.select(
   explode(
       split(df.value, " ")
   ).alias("word")
)

hashtags = words.filter(words.word.contains("#"))
hashtags = hashtags.select(lower(col('word')).alias('Symbol'))
hashtags = hashtags.select(regexp_replace(col('Symbol'), '#', '').alias('Symbol'))

hashtagFiltered = hashtags.join(ticker_df, "Symbol")

hashtagCounts = hashtagFiltered.groupby("Symbol").count()

query = hashtagCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

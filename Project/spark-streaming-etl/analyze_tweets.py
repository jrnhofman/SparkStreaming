from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession

from pyspark.sql.functions import explode, unix_timestamp
from pyspark.sql.functions import split, expr, lit
from pyspark.sql.functions import lower, col, regexp_replace, window

spark = SparkSession \
    .builder \
    .appName("TweetAndStockApp") \
    .getOrCreate()

# Get stock_tickers
ticker_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:9092") \
  .option("subscribe", "STOCK_QUOTES") \
  .option("startingOffsets", "earliest") \
  .load()

tickers = (ticker_df
    .withWatermark("timestamp", "2 hours")
    .select(
        col("timestamp")
        , split(col("value"), " ").getItem(0).alias("Symbol")
        , split(col("value"), " ").getItem(1).alias("Price")
    )
    .select(
        col("timestamp").alias("ticker_ts")
        , lower(col("Symbol")).alias("ticker_symbol")
        , col("Price").alias("price")
    )
    # Temporary since I changed the schema of the kafka data
    .filter(col("ticker_ts") > unix_timestamp(lit('2021-02-26 10:30:00')).cast('timestamp'))
)

tickers_with_window = (tickers
    .groupBy(
        window(col("ticker_ts"), "30 minutes", "30 minutes").alias("ticker_window"),
        col("ticker_symbol")
    )
    .agg({'price' : 'avg'})
    .select(
        col('ticker_window').start.alias('ticker_ts')
        , col('ticker_window')
        , col('ticker_symbol')
        , col('avg(price)').alias('price'))
)

# Get tweet stream from kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:9092") \
  .option("subscribe", "TWEETS") \
  .option("startingOffsets", "earliest") \
  .load()

# Split the lines into words
hashtags = (df
    .withWatermark("timestamp", "2 hours")
    .select(
        col("timestamp")
        , explode(split(col("value"), " ")
        ).alias("word")
    )
    .filter(col("word").contains("#"))
    .select(col('timestamp'), lower(col('word')).alias('Symbol'))
    .select(
        col('timestamp').alias('ht_ts')
        , regexp_replace(col('Symbol'), '#', '').alias('ht_symbol')
        , lit(1).alias('count')
    )
)

# hastag_counts = hashtags.groupBy(
#     window(col("ht_ts"), "30 minutes", "30 minutes").alias("ht_window"),
#     col("ht_symbol")
# ).count()

# # tickers = (ticker_df
# #     .withWatermark("timestamp", "14 days")
# #     .select(
# #         col("timestamp").alias('ticker_ts')
# #         , col("value")
# #     ))
# tickers = (ticker_df
#     .withWatermark("timestamp", "14 days")
#     .groupBy(
#         window(col("timestamp"), "30 minutes", "30 minutes").alias("ticker_window"),
#         col("Symbol").alias("ticker_symbol")
#     )
# ).avg()


joined = tickers_with_window.join(
    hashtags,
    expr("""
        ht_symbol = ticker_symbol
 AND
         ticker_ts <= ht_ts AND
         ticker_ts + interval 30 minutes > ht_ts

        """),
    "leftOuter"
)


query = joined \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

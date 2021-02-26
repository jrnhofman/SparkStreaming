from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession

from pyspark.sql.functions import explode, unix_timestamp
from pyspark.sql.functions import split, expr, lit, date_trunc
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
    .withWatermark("timestamp", "120 seconds")
    .select(
        col("timestamp")
        , split(col("value"), " ").getItem(0).alias("Symbol")
        , split(col("value"), " ").getItem(1).alias("Price")
    )
    .select(
        #date_trunc('minute', col("timestamp")).alias("ticker_ts")
        col("timestamp").alias("ticker_ts")
        , lower(col("Symbol")).alias("ticker_symbol")
        , col("Price").alias("price")
    )
    # Temporary since I changed the schema of the kafka data
    .filter(col("ticker_ts") > unix_timestamp(lit('2021-02-26 10:30:00')).cast('timestamp'))
)

# tickers_with_window = (tickers
#     .groupBy(
#         window(col("ticker_ts"), "30 minutes", "30 minutes").alias("ticker_window"),
#         col("ticker_symbol")
#     )
#     .agg({'price' : 'avg'})
#     .select(
#         col('ticker_window').start.alias('ticker_ts')
#         , col('ticker_window')
#         , col('ticker_symbol')
#         , col('avg(price)').alias('price'))
# )

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
    .withWatermark("timestamp", "120 seconds")
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
        , lit(1).alias('cnt')
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

# query = hashtags \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# query.awaitTermination()


joined = tickers.join(
    hashtags,
    expr("""
        ht_symbol = ticker_symbol
        AND
        ticker_ts <= ht_ts AND
        ticker_ts + interval 60 seconds > ht_ts
        """),
    "leftOuter"
)

### Doesn't work yet because of
# WARN UnsupportedOperationChecker: Detected pattern of possible 'correctness' issue due to global watermark. The query contains stateful operation which can emit rows older than the current watermark plus allowed late record delay, which are "late rows" in downstream stateful operations and these rows can be discarded. Please refer the programming guide doc for more details
result = (joined
    .groupBy(
        window(col("ticker_ts"), "1 minutes", "1 minutes").alias("ticker_window"),
        col("ticker_symbol")
    )
    .agg({'price' : 'avg', 'cnt' : 'sum'})
    .select(
        col('ticker_window').start.alias('ticker_ts')
        , col('ticker_window')
        , col('ticker_symbol')
        , col('avg(price)').alias('price')
        , col('sum(cnt)').alias('n_tweets')
    )
)

query = joined \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

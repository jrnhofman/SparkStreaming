import json
import pandas as pd
import datetime
import time
from tzlocal import get_localzone

from kafka import KafkaProducer
from yahoo_fin import stock_info as si


topic_name = 'STOCK_QUOTES'

starttime = time.time()

producer = KafkaProducer(
        bootstrap_servers=['broker:9092'])
print("Producer created")

def send_to_kafka(df):
    #producer = KafkaProducer(bootstrap_servers = util.get_broker_metadata())
    for k, row in df.iterrows():
        data_str = row.Symbol + ' ' + str(row.Price)
        print(data_str)
        producer.send(topic_name, data_str.encode())
        producer.flush()

def get_ticker_list():
    return pd.DataFrame(columns=['Symbol'], data=['AAPL', 'MSFT', 'AMZN', 'FB', 'GOOG', 'NVDA', 'ADBE'])
    #return pd.read_csv("NASDAQ_tickers.csv").iloc[:10]

def get_stock_quotes():
    tickers = get_ticker_list()
    while True:
        tickers['Price'] = tickers.apply(lambda x: si.get_live_price(x['Symbol']), axis=1)
        print(tickers)

        send_to_kafka(tickers)

        tickers['Price'] = None
        time.sleep(60.0 - ((time.time() - starttime) % 60.0))

if __name__ == "__main__":
    get_stock_quotes()

import json
import pandas as pd
import datetime
from time import sleep

from kafka import KafkaProducer
from yahoo_fin import stock_info as si


topic_name = 'STOCK_QUOTES'

producer = KafkaProducer(
        bootstrap_servers=['broker:9092'])
print("Producer created")

def send_to_kafka(df):
    #producer = KafkaProducer(bootstrap_servers = util.get_broker_metadata())
    for k, row in df.iterrows():
        producer.send(topic_name, str(dict(row)).encode())
        producer.flush()

def get_ticker_list():
    return pd.read_csv("NASDAQ_tickers.csv").iloc[:10]

def get_stock_quotes():
    print("HELLO")
    tickers = get_ticker_list()
    while True:
        tickers['Price'] = tickers.apply(lambda x: si.get_live_price(x['Symbol']), axis=1)
        tickers['Timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(tickers)

        send_to_kafka(tickers)

        tickers['Price'] = None
        tickers['Timestamp'] = None
        sleep(10)

if __name__ == "__main__":
    get_stock_quotes()

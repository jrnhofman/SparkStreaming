# Base image to start from.
FROM python:latest

RUN pip install kafka-python gcsfs fsspec yahoo_fin pandas tzlocal
RUN apt-get update
RUN apt-get install -y vim

COPY NASDAQ_tickers.csv /NASDAQ_tickers.csv
COPY get_stock_quotes.py /get_stock_quotes.py

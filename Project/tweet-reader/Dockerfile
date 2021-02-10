# Base image to start from.
FROM python:latest

# Information surrounding the creator.
LABEL maintainer="jrn.hofman@gmail.com"
LABEL description="Basic Kafka producer reading tweets and writing to Kafka topic."

RUN pip install tweepy kafka-python
RUN apt-get update
RUN apt-get install -y vim

ADD tweet_producer.py /server/
ADD get_authentication.py /server/
# Credentials
ADD twitter.txt /server/

WORKDIR /server/
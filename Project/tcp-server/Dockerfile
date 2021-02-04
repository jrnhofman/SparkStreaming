# Base image to start from.
FROM python:latest

# Information surrounding the creator.
LABEL maintainer="jrn.hofman@gmail.com"
LABEL description="Basic TCP server that reads tweets from twitter API and sends it to the client."

RUN pip install requests_oauthlib

ADD setup_tcp_server.py /server/

WORKDIR /server/
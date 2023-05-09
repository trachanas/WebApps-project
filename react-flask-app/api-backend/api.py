import time
import json
import configparser
import logging
import pandas as pd
import fastavro.schema
import io
import argparse
import snscrape.modules.twitter as sntwitter
import time
from langdetect import detect
from flask_cors import CORS, cross_origin
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

app = Flask(__name__)
cors = CORS(app)

#config.read('application_properties')
#ip = config.get('database', 'ip')
ip = '127.0.0.1'
port = '9092'
url = ip + ':' + port
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


@app.route('/time')
def get_current_time():
    return {'time': time.time()}

@app.route('/hello')
def get_message():
    return {'message' : 'hello'}

@app.route('/api/endpoint', methods=['POST'])
@cross_origin(origin='*', headers=['Content-Type'])
def handle_post_request():
    data = request.json
    print(data)
    producer = KafkaProducer(bootstrap_servers=[url])
    topic_name = 'hello'
    publish_message(topic_name)
    return {"Success" : "ssss"}




def publish_message(topic_name):
    try:
        start_time = time.perf_counter()
        print("this is the topic name: {}".format(topic_name))
        df = retrieve_tweets(topic_name, threshold=10)
        end_time = time.perf_counter()
        tweets_retrieval_time = round(end_time - start_time, 5)
        logger.info(f'Tweets dataset retrieved in {tweets_retrieval_time} secs.')
        json_str = df.to_json()
        start_time = time.perf_counter()
        #producer.send(topic_name, json_str.encode('utf-8'))
        end_time = time.perf_counter()
        sending_time = round(end_time - start_time, 5)
        producer.flush()
        logger.info(f'Message published successfully in {sending_time} secs.')
    except Exception as ex:
        logger.error('Exception in publishing message.')
        logger.error(str(ex))



def retrieve_tweets(topic_name, threshold):
    tweets_list = list()
    search_query = f'from:{topic_name}'
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search_query).get_items()):
        if i >= threshold:
            break
        lang = detect(tweet.rawContent)
        if lang == 'en':
            tweets_list.append([tweet.date, tweet.user.username, tweet.rawContent])
    tweets_df = pd.DataFrame(tweets_list, columns=['datetime', 'username', 'text'])
    return tweets_df





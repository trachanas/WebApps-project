from kafka import KafkaProducer, KafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from kafka.errors import KafkaError
import json
import configparser
import logging
import time
import argparse
from preprocessing import preCleanEnglishText
import pymongo
import pandas as pd
from textblob import TextBlob


def convert_record_to_df(record):
    rows = []
    for message in record.values():
        for m in message:
            for i in range(len(m.value['datetime'])):
                row = {
                    'datetime': m.value['datetime'][str(i)],
                    'username': m.value['username'][str(i)],
                    'text' : m.value['text'][str(i)]
                }
                rows.append(row)
# Create Pandas DataFrame from list of rows
    df = pd.DataFrame(rows)
    print(df)
    return df


def extract_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(text)
    print(scores)
    if scores.get('compound') > 0:
        return 'POSITIVE'
    elif scores.get('compound') < 0:
        return 'NEGATIVE'
    else:
        return 'NEUTRAL'




# Connect to the MongoDB database
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['tweets_db']

config = configparser.ConfigParser()
config.read('application_properties')
ip = config.get('database', 'ip')
port = config.get('database', 'port')
url = ip + ':' + port
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s--%(levelname)s--%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info('Consumer listens to ' + url)
# Create the parser
parser = argparse.ArgumentParser(description='Description of your script')

parser.add_argument('--topic_name', type=str)
args = parser.parse_args()

# Access the values of the arguments
topic_name = args.topic_name

# Set up the Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[url],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Subscribe to the topic and consume messages
collection = db['tweets_collection']

try:
    while True:
        message = consumer.poll(timeout_ms=30000)
        if message:
            df = convert_record_to_df(message)
            if not df.empty:
                df = preCleanEnglishText(df, 'text', 'text')
                df['sentiment'] = df['text'].apply(lambda x: extract_sentiment(x))
                start_time = time.perf_counter()
                logger.info('Consumer received a message.')
                # # Insert the data into a collection
                data = df.to_dict('records')
                if len(data) > 0:
                    collection.insert_many(data)
                end_time = time.perf_counter()
                time_consumed = round(end_time - start_time, 5)
                logger.info('Consumer read a message in ' + str(time_consumed) + ' seconds.')
except Exception as ex:
    consumer.close()
    logger.error('Exception in consuming message.')
    logger.error(str(ex))

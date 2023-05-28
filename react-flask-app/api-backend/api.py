import time
import json
import logging
import pandas as pd
import time
from langdetect import detect
from flask_cors import CORS, cross_origin
from flask import Flask, request, jsonify, make_response

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from bson import json_util
import random
import threading
from amazon_reviews_scraper import get_product_name, \
get_product_reviews_by_name, AmazonScraperTool



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

amz_scraper = AmazonScraperTool()



@app.route('/product_name/<path:link_to_product>', methods=['GET'])
@cross_origin() 
def get_product_name_request(link_to_product):
    return {'product_name' : get_product_name(link_to_product)}


@app.route('/product_with_all_reviews/<path:link_to_product>', methods=['GET'])
def get_product_reviews_by_name_request(link_to_product):
    product_name = get_product_name(link_to_product)
    result = get_product_reviews_by_name(product_name)
    json_data = json.loads(json_util.dumps(result))
    return json_data


def process_analysis_request(product_url):
    amz_scraper.scrape_reviews(product_url)


@app.route('/start_analysis/<path:product_url>', methods=['POST'])
@cross_origin() 
def start_analysis_request(product_url):
    thread = threading.Thread(target=process_analysis_request, args=(product_url,))
    thread.start()
    print(product_url)
    response_data = {
        'message': 'Success',
        'data': { 'product_url' : product_url}
    }
    response = make_response(jsonify(response_data), 200)
    print('******** Request analysis processing started in a new thread. ********')
    return response
   
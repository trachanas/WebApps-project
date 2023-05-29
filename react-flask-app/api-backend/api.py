import time
import json
import logging
import pandas as pd
import time
from langdetect import detect
from flask_cors import CORS, cross_origin
from flask import Flask, request, jsonify, make_response
import requests
from bs4 import BeautifulSoup
import pandas as pd
#from urllib.parse import urlencode
import csv

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from bson import json_util
import random
import threading
from amazon_reviews_scraper import get_product_name, \
get_product_reviews_by_name, AmazonScraperTool, \
get_topN_negative_products, get_topN_positive_products


app = Flask(__name__)
cors = CORS(app)

amz_scraper = AmazonScraperTool()



def process_analysis_request(product_url):
    print("START process_analysis_request")
    amz_scraper.scrape_reviews(product_url)
    print("END process_analysis_request")
    


@app.route('/start_analysis', methods=['POST'])
@cross_origin() 
def start_analysis_request():
    json_data = request.json
    product_url = json_data.get('product_url')
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


@app.route('/product_name', methods=['GET'])
@cross_origin() 
def get_product_name_request():
    json_data = request.json
    product_url = json_data.get('product_url')
    return {'product_name' : get_product_name(product_url)}


@app.route('/product_with_all_reviews', methods=['GET'])
def get_product_reviews_by_name_request():
    json_data = request.json
    product_url = json_data.get('product_url')
    product_name = get_product_name(product_url)
    result = get_product_reviews_by_name(product_name)
    if result is None:
        print(f'Product with name {product_name} does not exist in DB')
        return None
    json_data = json.loads(json_util.dumps(result))
    return json_data



@app.route('/top_N_positive/<topN>', methods=['GET'])
def get_topN_positive_request(topN):
    result = get_topN_positive_products(int(topN))
    json_data = json.loads(json_util.dumps(result))
    return json_data


@app.route('/top_N_negative/<topN>', methods=['GET'])
def get_topN_negative_request(topN):
    result = get_topN_negative_products(int(topN))
    json_data = json.loads(json_util.dumps(result))
    return json_data


   
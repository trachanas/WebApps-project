import re
import time
import requests
from bs4 import BeautifulSoup
import yaml
from langdetect import detect
from pymongo import MongoClient
from sentiment_analysis import Analyzer, overall_sentiment_analysis


with open('app_properties.yml', 'r') as file:
    properties = yaml.safe_load(file)



def connect_to_mongodb(properties):
    database_uri = properties['database']['database_uri']
    database_name = properties['database']['database_name']
    database_collection_name = properties['database']['database_collection_name']
    client = MongoClient(database_uri)
    db = client[database_name]
    collection = db[database_collection_name]
    return collection


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'TE': 'Trailers'
}

threshold = properties['amazon_reviews_scraper']['threshold']
analyzer = Analyzer()



def isValidUrl(url):
    # URL with 'https://amazon.com' format in front
    if not re.search(r'^https?://www\.amazon\.com', url):
        return False
    # URL containing valid ASIN number
    if not re.search(r'/(\w{10})/', url):
        return False
    return True


def create_review_url(url):
    amazon_url = properties['app']['amazon_url']
    product_name = url.split(amazon_url)[1].split('/')[0]
    pr_url = properties['amazon_reviews_scraper']['pr_url']
    asin = re.search(r'/(\w{10})/', url).group(1)
    rest_url = properties['amazon_reviews_scraper']['rest_url']
    url = amazon_url + product_name + '/' + pr_url + '/' + asin + '/' + rest_url
    return url


def get_product_name(url):
    amazon_url = properties['app']['amazon_url']
    product_name = url.split(amazon_url)[1].split('/')[0]
    return product_name


def get_product_reviews_by_name(product_name):
    collection = connect_to_mongodb(properties)
    filter_criteria = {'product_name': product_name}
    count = collection.count_documents(filter_criteria)
    if count > 0:
        result = collection.find_one(filter_criteria)
    else: 
        result = None 
    return result


def get_topN_positive_products(properties, top_n):
    collection = connect_to_mongodb(properties)
    pipeline = [
        {'$sort': {'positive': -1}},  # Sort in descending order of 'positive' values
        {'$limit': top_n}
    ]
    result = collection.aggregate(pipeline)
    print(result)
    return result


def get_topN_negative_products(properties, top_n):
    collection = connect_to_mongodb(properties)
    pipeline = [
        {'$sort': {'negative': -1}},  # Sort in descending order of 'positive' values
        {'$limit': top_n}
    ]
    result = collection.aggregate(pipeline)
    print(result)
    return result

def insert_reviews_db(properties, product_reviews, product_name):
    collection = connect_to_mongodb(properties)
    if collection.count_documents({'product_name': product_name}) == 0:
        collection.insert_one(product_reviews)
    else:
        current_entry = collection.find_one({'product_name': product_name})
        current_entry['positive'] += product_reviews['positive']
        current_entry['negative'] += product_reviews['negative']
        current_entry['neutral'] += product_reviews['neutral']

        number_of_reviews = current_entry['positive'] + current_entry['negative'] + current_entry['neutral']
        current_entry['positive_percentage'] = current_entry['positive'] / number_of_reviews
        current_entry['negative_percentage'] = current_entry['negative'] / number_of_reviews

        collection.update_one({'product_name': product_name},
                              {'$set': {'positive': current_entry['positive'],
                                        'negative': current_entry['negative'],
                                        'neutral': current_entry['neutral'],
                                        'positive_percentage': current_entry['positive_percentage'],
                                        'negative_percentage': current_entry['negative_percentage']
                                        }})



def hasNextPage(soup):
    a_tag = soup.select_one('li.a-last > a[href]')
    if a_tag:
        return True if a_tag.has_attr('href') else False
    else:
        return False


def get_sentiment_analysis_in_batch(product_name, one_page_reviews, analyzer):
    entry = {'product_name': product_name,
             'positive': 0,
             'negative': 0,
             'neutral': 0,
             'positive_percentage': 0.0,
             'negative_percentage': 0.0
             }
    for text in one_page_reviews:
        sentiment_polarity = analyzer.one_review_sentiment_analysis(text)
        if sentiment_polarity > 0:
            entry['positive'] += 1
        elif sentiment_polarity < 0:
            entry['negative'] += 1
        else:
            entry['neutral'] += 1

    number_of_reviews_in_batch = len(one_page_reviews)
    entry['positive_percentage'] = entry['positive'] / number_of_reviews_in_batch
    entry['negative_percentage'] = entry['negative'] / number_of_reviews_in_batch
    return entry


def getReviewContent(url, response, soup):
    review_list = soup.find('div', {'id': 'cm_cr-review_list'})
    reviews = []
    try:
        product_reviews = review_list.find_all('div', {'data-hook': 'review'})  # return reviews
        if len(product_reviews) == 0:
            print('Product does not have reviews.')
            return None
        print(f"Returned {len(product_reviews)} reviews.")
        for product_review in product_reviews:
            review_body = product_review.find('span', {'data-hook': 'review-body'}).text.strip()
            language = detect(review_body)
            if language != 'en':
                continue
            reviews.append(review_body)
    except Exception as e:
        print(f'Exception raised: {e}')
    return reviews


class AmazonScraperTool:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(headers)

    def scrape_reviews(self, url, filter_by='recent'):
        all_reviews = list()
        collection = connect_to_mongodb(properties)
        product_name = get_product_name(url)
        no_of_documents = collection.count_documents({'product_name': product_name})
        if no_of_documents > 0:
            print(f'Document for {product_name} already exists!')
        try:
            if isValidUrl(url):

                product_reviews = {
                    'product_name': product_name,
                    'positive': 0, 'negative': 0, 'neutral': 0,
                    'positive_percentage': 0.0, 'negative_percentage': 0.0
                }
                reviews_url = create_review_url(url)
                pn_url = properties['amazon_reviews_scraper']['pn_url']
                reviews_url = reviews_url + pn_url
                for i in range(1, threshold + 1):
                    one_page_reviews = list()
                    reviews_url_with_pn = reviews_url + str(i)
                    print(f'GET reviews from: {reviews_url_with_pn}.')
                    response = self.session.get(reviews_url_with_pn)
                    if response.status_code != 200:
                        raise ConnectionException(f'ConnectionException: Response\'s status code = {response.status_code}')
                    soup = BeautifulSoup(response.content, 'html.parser')
                    one_page_reviews = getReviewContent(reviews_url_with_pn, response, soup)
                    product_reviews = get_sentiment_analysis_in_batch(product_name, one_page_reviews, analyzer)
                    print(product_reviews)
                    insert_reviews_db(properties, product_reviews, product_name)

                    if one_page_reviews is None:
                        return all_reviews
                    all_reviews.extend(one_page_reviews)
                    time.sleep(10)
                    if not hasNextPage(soup):
                        print('Review URL does not have next page.')
                        return all_reviews
            else:
                raise InvalidUrlException('InvalidUrlException: URL is invalid.')
        except InvalidUrlException as e:
            print(e.message)
        except Exception as e:
            print(f"Raised exception: {e}")
            return all_reviews
        finally:
            return all_reviews

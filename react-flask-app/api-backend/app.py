import time
import pandas as pd
import re
from amazon_reviews_scraper import AmazonScraperTool
from sentiment_analysis import Analyzer, overall_sentiment_analysis
import yaml

with open('app_properties.yml', 'r') as file:
    properties = yaml.safe_load(file)


def create_file_name(url):
    amazon_url = properties['app']['amazon_url']
    product_name = url.split(amazon_url)[1].split('/')[0]
    asin = re.search(r'/(\w{10})/', url).group(1)
    product_name = product_name.replace('-', '_') + '_' + asin
    return product_name

reviews = []
amz_scraper = AmazonScraperTool()

#product_url = 'https://www.amazon.com/Big-Book-Dashboards-Visualizing-Real-World/product-reviews/1119282713/ref=cm_cr_arp_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&pageNumber=1'
#product_url = 'https://www.amazon.com/Simple-Deluxe-Office-Computer-Ergonomic/product-reviews/B0BHQPL48C/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
#product_url = 'https://www.amazon.com/Apple-iPhone-12-Pro-Graphite/product-reviews/B09JF5ZHQS/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
#many reviews
#product_url = 'https://www.amazon.com/SanDisk-3-Pack-Ultra-Flash-3x32GB/dp/B08HSS37H7/ref=sr_1_18?qid=1683477118&s=computers-intl-ship&sr=1-18&th=1'
#no reviews
#product_url = 'https://www.amazon.com/Grosgrain-74200-Ribbon-White/dp/B07WFJ6G34/ref=sr_1_1?qid=1683485407&s=arts-crafts-intl-ship&sr=1-1'
#26 reviews
product_url = 'https://www.amazon.com/ASUS-Lightweight-Programmable-Ultralight-Resistance/dp/B09QRXBPQL/ref=cm_cr_arp_d_product_top?ie=UTF8'

#product_url = 'https://www.amazon.com/Razer-Ornata-Gaming-Keyboard-Spill-Resistant/dp/B09X6FKCBD/ref=sr_1_1_sspa?keywords=gaming+keyboard&pd_rd_r=ca6295ab-92f3-4900-8f84-7ab37ee41798&pd_rd_w=XMjgH&pd_rd_wg=DZhRp&pf_rd_p=12129333-2117-4490-9c17-6d31baf0582a&pf_rd_r=7Y67STY6N1BN8FF3BPVV&qid=1684073723&sr=8-1-spons&psc=1&spLa=ZW5jcnlwdGVkUXVhbGlmaWVyPUEzQ05HSUFLQlVWVUhCJmVuY3J5cHRlZElkPUEwNDE3NzA5MzcxQjhMQ1JJSjUwQSZlbmNyeXB0ZWRBZElkPUEwNDQwNDYwMkdWT05JU0VWQklSNSZ3aWRnZXROYW1lPXNwX2F0ZiZhY3Rpb249Y2xpY2tSZWRpcmVjdCZkb05vdExvZ0NsaWNrPXRydWU='

content = amz_scraper.scrape_reviews(product_url)

df = pd.DataFrame({})
if (content is not None) and len(content) > 0:
    #print(f'len is {len(content)}')
    df = pd.DataFrame(content, columns=['review_text'])

df = df.reset_index()
file_name = create_file_name(product_url)
print(file_name)
file_name = './product_reviews/' + file_name + '_reviews.csv'

analyzer = Analyzer()
#df = analyzer.overall_scores_sentiment_analysis(df)
#df = overall_sentiment_analysis(df)


df.to_csv(file_name, index=False)

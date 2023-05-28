import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer
from preprocessing import preCleanEnglishText
import yaml

with open('app_properties.yml', 'r') as file:
    properties = yaml.safe_load(file)



def review_sentiment(polarity):
    if polarity > 0:
        return 'POSITIVE'
    elif polarity < 0:
        return 'NEGATIVE'
    else:
        return 'NEUTRAL'


def overall_sentiment_analysis(df):
    if df is None and df.empty:
        print(f"Reviews dataframe is null or empty.")
        return None
    for index, row in df.iterrows():
        polarity = df.at[index, 'polarity']
        df.at[index, 'sentiment'] = review_sentiment(polarity)
    return df





class Analyzer:
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()

    def one_review_sentiment_analysis(self, text):
        text = str(text)
        sentiment_scores = self.analyzer.polarity_scores(text)
        return sentiment_scores['compound']


    def overall_scores_sentiment_analysis(self, df):
        df['polarity'] = ''
        if df is None and df.empty:
            print(f"Reviews dataframe is null or empty.")
            return None

        df = preCleanEnglishText(df, 'review_text', 'clean_review_text')
        for index, row in df.iterrows():
            text = df.at[index, 'clean_review_text']
            df.at[index, 'polarity'] = self.one_review_sentiment_analysis(text)
        return df


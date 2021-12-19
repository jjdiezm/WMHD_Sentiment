# Coded by jdiezm@uoc.edu to perform 2021 TFM on WMHD Sentiment Analysis

import DBAccessHelper
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from collections import Counter


def calc_percentage(dividend, divisor):
    """ Function to calculate percents as dividend/divisor * 100

    :param dividend: Dividend of the operation
    :param divisor: Divisor of the operation
    :return: percent value
    """
    return dividend/divisor * 100


def return_all_df(param_year="all", param_account="all", param_tweet="all"):
    """ Function to call method that returns DF from SQL Query on DB

    :param param_year: optional string type to indicate "all" to not filter or the year like "2021" to filter by year
    :param param_account: optional string type to indicate "all" to not filter or "certified" to filter by certified
                          accounts with more than 5000 followers
    :param param_tweet: optional string type to indicate:
                          - "all" to not filter
                          - "relevant" to filter on tweets with more than 500 retweets and referenced_tweets_count = 0
                          - "liked" to filter on tweets with more than 500 likes and referenced_tweets_count = 0
    :return: Pandas DataFrame
    """

    mode_test = True
    db_instance = DBAccessHelper.DBAccessHelper(mode_test)
    if param_account == "certified":
        sql = """SELECT tbl_Tweets.Id, tbl_Tweets.fk_author, tbl_Tweets.created_at, tbl_Tweets.original_text, 
                        tbl_Tweets.in_reply_to_user, tbl_Tweets.source, tbl_Tweets.lang, tbl_Tweets.retweet_count, 
                        tbl_Tweets.reply_count, tbl_Tweets.like_count, tbl_Tweets.quote_count, 
                        tbl_Tweets.possibly_sensitive, tbl_Tweets.cleaned_text, tbl_Tweets.referenced_tweets_count, 
                        tbl_Tweets.vader_sentiment, tbl_Tweets.textblob_sentiment, tbl_Tweets.text_reduced, 
                        tbl_Authors.verified, tbl_Authors.shown_name, tbl_Authors.followers_count, 
                        tbl_Authors.tweet_count
                 FROM tbl_Tweets INNER JOIN tbl_Authors ON tbl_Tweets.fk_author = tbl_Authors.Id
                 WHERE tbl_Tweets.lang='en' AND tbl_Authors.verified=True AND tbl_Authors.followers_count>5000"""
    else:
        sql = "SELECT * FROM tbl_Tweets WHERE lang='en'"
    if param_year != "all":
        start_date = param_year + "-09-09T00:00:00.000Z"
        end_date = param_year + "-11-11T00:00:00.000Z"
        sql = sql + f" AND tbl_Tweets.created_at > '{start_date}' AND tbl_Tweets.created_at < '{end_date}'"
    if param_tweet == "relevant":
        sql = sql + f" AND tbl_Tweets.retweet_count > 500 AND tbl_Tweets.referenced_tweets_count = '0'"
    if param_tweet == "liked":
        sql = sql + f" AND tbl_Tweets.like_count > 500 AND tbl_Tweets.referenced_tweets_count = '0'"
    return db_instance.query_to_df(sql)


def return_cleaned_text_df(param_year="all"):
    """ Function to call method that returns DF from SQL Query on DB, restricted to field cleaned_text

    :param param_year: optional string type to indicate "all" to not filter or the year like "2021" to filter by year
    :return: Pandas DataFrame
    """
    mode_test = True
    db_instance = DBAccessHelper.DBAccessHelper(mode_test)
    sql = "SELECT cleaned_text FROM tbl_Tweets WHERE lang='en'"
    if param_year != "all":
        start_date = param_year + "-09-09T00:00:00.000Z"
        end_date = param_year + "-11-11T00:00:00.000Z"
        sql = sql + f" AND created_at > '{start_date}' AND created_at < '{end_date}'"
    return db_instance.query_to_df(sql)


def print_general(param_account="all", param_tweet="all"):
    """ Print general information of the DataFrame

    :param param_account: optional string type to indicate "all" to not filter or "certified" to filter by certified
                          accounts with more than 5000 followers
    :param param_tweet: optional string type to indicate:
                          - "all" to not filter
                          - "relevant" to filter on tweets with more than 500 retweets and referenced_tweets_count = 0
                          - "liked" to filter on tweets with more than 500 likes and referenced_tweets_count = 0
    :return: Nothing
    """
    for year_filter in ["all", "2019", "2020", "2021"]:
        tweets_df = return_all_df(year_filter, param_account, param_tweet)
        tweets_df['text_len'] = tweets_df['original_text'].astype(str).apply(len)
        tweets_df['text_word_count'] = tweets_df['original_text'].apply(lambda x: len(str(x).split()))
        print(f"\nData from {year_filter} tweets")
        print("Total tweets ", tweets_df.count())
        print("Average length of tweets ", round(np.mean(tweets_df['text_len'])))
        print("Average word counts of tweets", round(np.mean(tweets_df['text_word_count'])))


def graphic_general():
    """ Graphic general information of the DataFrame

    :return: Nothing
    """
    for year_filter in ["all", "2019", "2020", "2021"]:
        tweets_df = return_all_df(year_filter)
        tweets_df['text_len'] = tweets_df['original_text'].astype(str).apply(len)
        tweets_df['text_word_count'] = tweets_df['original_text'].apply(lambda x: len(str(x).split()))
        title1 = f'Histogram by length of tweets ({year_filter})'
        tweets_df['text_len'].plot(bins=100, kind='hist', title=title1)
        plt.show()
        title2 = f'Histogram by the word counts of all tweets ({year_filter})'
        tweets_df['text_word_count'].plot(bins=100, kind='hist', title=title2)
        plt.show()


def print_sentiment(param_account="all", param_tweet="all"):
    """ Print an generate graphics for sentiment analysis information in the DataFrame

    :param param_account: optional string type to indicate "all" to not filter or "certified" to filter by certified
                              accounts with more than 5000 followers
    :param param_tweet: optional string type to indicate:
                          - "all" to not filter
                          - "relevant" to filter on tweets with more than 500 retweets and referenced_tweets_count = 0
                          - "liked" to filter on tweets with more than 500 likes and referenced_tweets_count = 0
    :return: Nothing
    """
    for year_filter in ["all", "2019", "2020", "2021"]:
        tweets_df = return_all_df(year_filter, param_account, param_tweet)
        for type_graphic in ["Vader", "TextBlob"]:
            title = f"{year_filter.capitalize()} Sentiment Analysis Result for {type_graphic} Analysis " \
                    f"considering {param_account} users & {param_tweet} tweets"
            if type_graphic == "Vader":
                positive = len(tweets_df[tweets_df["vader_sentiment"] == "positive"])
                neutral = len(tweets_df[tweets_df["vader_sentiment"] == "neutral"])
                negative = len(tweets_df[tweets_df["vader_sentiment"] == "negative"])
            else:
                positive = len(tweets_df[tweets_df["textblob_sentiment"] == "positive"])
                neutral = len(tweets_df[tweets_df["textblob_sentiment"] == "neutral"])
                negative = len(tweets_df[tweets_df["textblob_sentiment"] == "negative"])
            positive_percent = calc_percentage(positive, len(tweets_df))
            neutral_percent = calc_percentage(neutral, len(tweets_df))
            negative_percent = calc_percentage(negative, len(tweets_df))
            print(title)
            print("Positive count: {} - percent: {}%".format(positive,  format(positive_percent, '.1f')))
            print("Neutral count: {} - percent: {}%".format(neutral, format(neutral_percent, '.1f')))
            print("Negative count: {} - percent: {}%".format(negative, format(negative_percent, '.1f')))
            print("\n\nPreparing graphical window...\n")
            labels = ['Positive ['+str(positive_percent)+'%]', 'Neutral ['+str(neutral_percent)+'%]',
                      'Negative ['+str(negative_percent)+'%]']
            sizes = [positive, neutral, negative]
            colors = ['yellowgreen', 'blue', 'red']
            patches, texts = plt.pie(sizes, colors=colors, startangle=90)
            plt.style.use('default')
            plt.legend(labels)
            plt.title(title)
            plt.axis('equal')
            plt.show()


def remove_synthetic_words(text):
    """ Function to remove synthetic words used in cleaned_text @USER, URL... and the main HASHTAGS

    :param text: Text to remove words from
    :return: new text processed and cleaned
    """
    synthetic_words = ['@user', 'url', '#WorldMentalHealthDay', '#WorldMentalHealthDay2021',
                       '#WorldMentalHealthDay2020', '#WorldMentalHealthDay2019']
    text_words = text.split()
    result_words = [word for word in text_words if word.lower() not in synthetic_words]
    result = ' '.join(result_words)
    return result


def paint_word_cloud(text, title):
    """ Function to paint the WordClouds

    :param text: Text to analyze and extract the WordCloud
    :param title: Text to title the WordCloud representation
    :return: Nothing
    """
    stopwords = set(STOPWORDS)
    wc = WordCloud(background_color="white", max_words=3000, stopwords=stopwords, repeat=True)
    wc.generate(remove_synthetic_words(str(text)))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis("off")
    plt.title(title)
    plt.show()


def generate_main_cloud(param_type, algorithm="Vader"):
    """ Function to Create a WordCloud by setting the dataset and calling the paint function

    :param param_type: Type of WordCloud in
                        - main: WordCloud of all the dataframe
                        - positive: WordCloud of positive sentiment dataset
                        - negative: WordCloud of negative sentiment dataset
                        - neutral: WordCloud of neutral sentiment dataset
                        - all: Generate all WordClouds
    :param algorithm: Which Analysis Vader or BlobText (does not affect to main WordCloud)
    :return: Nothing
    """
    if algorithm == "Vader":
        sentiment_field = "vader_sentiment"
    else:
        sentiment_field = "textblob_sentiment"
    for year_filter in ["all", "2019", "2020", "2021"]:
        tweets_df = return_all_df(year_filter)
        if param_type == "main":
            paint_word_cloud(tweets_df["cleaned_text"].values, f"Main WordCloud for {year_filter}")
        elif param_type == "positive":
            tweet_pos = tweets_df[tweets_df[sentiment_field] == "positive"]
            paint_word_cloud(tweet_pos["cleaned_text"].values,
                             algorithm + f" positive sentiment WordCloud for {year_filter}")
        elif param_type == "negative":
            tweet_neg = tweets_df[tweets_df[sentiment_field] == "negative"]
            paint_word_cloud(tweet_neg["cleaned_text"].values,
                             algorithm + f" negative sentiment WordCloud for {year_filter}")
        elif param_type == "neutral":
            tweet_neu = tweets_df[tweets_df[sentiment_field] == "neutral"]
            paint_word_cloud(tweet_neu["cleaned_text"].values,
                             algorithm + f" neutral sentiment WordCloud for {year_filter}")
        else:
            tweet_pos = tweets_df[tweets_df[sentiment_field] == "positive"]
            tweet_neg = tweets_df[tweets_df[sentiment_field] == "negative"]
            tweet_neu = tweets_df[tweets_df[sentiment_field] == "neutral"]
            paint_word_cloud(tweets_df["cleaned_text"].values, f"Main WordCloud for {year_filter}")
            paint_word_cloud(tweet_pos["cleaned_text"].values,
                             algorithm + f" positive sentiment WordCloud for {year_filter}")
            paint_word_cloud(tweet_neg["cleaned_text"].values,
                             algorithm + f" negative sentiment WordCloud for {year_filter}")
            paint_word_cloud(tweet_neu["cleaned_text"].values,
                             algorithm + f" neutral sentiment WordCloud for {year_filter}")


def return_reduced_text_df(param_filter="all", param_algorithm="Vader", param_year="all"):
    """ Function to call method that returns DF from SQL Query on DB, restricted to field text_reduced

    :param param_filter: define whether to filter tweets values:
                      - all: returns all the tweets
                      - positive: only tweets Vader positive
                      - negative: only tweets Vader negative
                      - neutral: only tweets Vader neutral
    :param param_algorithm: define on what algorithm sentiment is based to create the list
    :param param_year: optional string type to indicate "all" to not filter or the year like "2021" to filter by year
    :return: Pandas DataFrame
    """
    mode_test = True
    db_instance = DBAccessHelper.DBAccessHelper(mode_test)
    if param_algorithm == "Vader":
        sentiment_field = "vader_sentiment"
    else:
        sentiment_field = "textblob_sentiment"
    sql = "SELECT text_reduced FROM tbl_Tweets WHERE lang='en'"
    if param_filter in ["positive", "negative", "neutral"]:
        sql = sql + f" AND {sentiment_field}='{param_filter}'"
    if param_year != "all":
        start_date = param_year + "-09-09T00:00:00.000Z"
        end_date = param_year + "-11-11T00:00:00.000Z"
        sql = sql + f" AND created_at > '{start_date}' AND created_at < '{end_date}'"
    return db_instance.query_to_df(sql)


def most_common_words(param_filter="all", param_algorithm="Vader"):
    """ Function to calculate Most Common Words and them numbers

    :param param_filter: define whether to filter tweets values:
                      - all: returns all the tweets
                      - positive: only tweets Vader positive
                      - negative: only tweets Vader negative
                      - neutral: only tweets Vader neutral
    :param param_algorithm: define on what algorithm sentiment is based to create the list
    :return: Nothing
    """
    for year_filter in ["all", "2019", "2020", "2021"]:
        tweets_df = return_reduced_text_df(param_filter, param_algorithm, year_filter)
        list_words = Counter(" ".join(tweets_df["text_reduced"]).split()).most_common(20)
        print("*************************************")
        print(f"\nLIST OF WORDS: {param_filter} sentiment values based on {param_algorithm} from {year_filter}\n")
        for element in list_words:
            print("{} appeared: {} times".format(element[0], element[1]))

# Coded by jdiezm@uoc.edu to perform 2021 TFM on WMHD Sentiment Analysis

import json
import requests
import datetime
import DBAccessHelper
from retrying import retry
from auth_twitter import *
from requests.structures import CaseInsensitiveDict

W_R_MIN = 1000
W_R_MAX = 2000
S_M_A_N = 8

# World Mental Health Day is 2021-10-10
event_day = datetime.datetime(2021, 10, 10)
base_url = "https://api.twitter.com/2/tweets/"
token = BEARER_TOKEN


@retry(wait_random_min=W_R_MIN, wait_random_max=W_R_MAX, stop_max_attempt_number=S_M_A_N)
def search_tweets_full_archive(string_query, param_start_time, param_end_time, param_next_token=""):
    """ Function to get the hourly Tweet count on a determined string query between dates

    :param string_query: String with the query to look for, example: #WorldMentalHealthDay2021 -is:retweet lang:en
    :param param_start_time: Date from you're looking for tweets, UTC timestamp, example: 2019-09-10T00:00:00Z
    :param param_end_time: Date to you're looking for tweets, UTC timestamp, example: 2021-11-10T23:59:59Z
    :param param_next_token: This parameter is used to get the next page of results. The value used with the parameter
                             is pulled directly from the response provided by the API, and should not be modified.
    :return: json with the list of start dates and end dates and tweets registered, and next_token if exists
    Example of return:
        {
        "data": [
            {
                "end": "2019-10-10T19:00:00.000Z",
                "start": "2019-10-10T18:00:00.000Z",
                "tweet_count": 6577
            },
            {
                "end": "2019-10-10T19:59:59.000Z",
                "start": "2019-10-10T19:00:00.000Z",
                "tweet_count": 4641
            }
        ],
        "meta": {
            "total_tweet_count": 17873,
            "next_token": "1jzu9lk96azp0b3x7888hrpurii9ub5dprcdvepryflp"
            }
        }
    """

    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer " + token
    url = base_url + 'counts/all'
    print(url)
    params = dict()
    params['query'] = string_query
    params['start_time'] = param_start_time
    params['end_time'] = param_end_time
    if param_next_token != "":
        params['next_token'] = param_next_token
    response = requests.get(url,
                            params=params,
                            headers=headers)
    return response.text


def search_wmhd_tweets_years(year_start=2019, year_end=2021):
    """ Function to hourly account tweets and save in .json files of tweets related to World Mental Health Day
        from year_start to year_end, on a 2 month period per year from sept-10th to nov-10th.
        WMHD is on oct-10th every year.

    :param year_start: Optional parameter Integer value to begin the search. Value Default: 2019
    :param year_end: Optional parameter Integer value to finish the search. Value Default: 2021
    :return: json files named 2month_tweets_year_integer, integer is 0 or 1 due to the Twitter API needs 2 calls
             to return a 2 month period. The JSON contains the list of start dates and end dates and tweets registered,
             and next_token if exists. Example of file content:
                {
                "data": [
                    {
                        "end": "2019-10-10T19:00:00.000Z",
                        "start": "2019-10-10T18:00:00.000Z",
                        "tweet_count": 6577
                    },
                    {
                        "end": "2019-10-10T19:59:59.000Z",
                        "start": "2019-10-10T19:00:00.000Z",
                        "tweet_count": 4641
                    }
                ],
                "meta": {
                    "total_tweet_count": 17873,
                    "next_token": "1jzu9lk96azp0b3x7888hrpurii9ub5dprcdvepryflp"
                    }
                }
    """

    current_year = year_start
    while current_year <= year_end:
        year = str(current_year)
        file_number = 0
        query = "#WorldMentalHealthDay" + year + " OR #WorldMentalHealthDay -is:retweet lang:en"
        start_time = year + "-09-10T00:00:00Z"
        end_time = year + "-11-10T23:59:59Z"
        str_search = search_tweets_full_archive(query, start_time, end_time)
        file_name = "2month_tweets_" + year + "_" + str(file_number) + ".json"
        with open(file_name, 'w') as file_handle:
            file_handle.write(str_search)
        file_number += 1
        json_search = json.loads(str_search)
        results = json_search.get('meta')
        if 'next_token' in results:
            next_token = results.get('next_token')
        else:
            next_token = ""
        while next_token != "":
            str_search = search_tweets_full_archive(query, start_time, end_time, next_token)
            file_name = "2month_tweets_" + year + "_" + str(file_number) + ".json"
            with open(file_name, 'w') as file_handle:
                file_handle.write(str_search)
            file_number += 1
            json_search = json.loads(str_search)
            results = json_search.get('meta')
            if 'next_token' in results:
                next_token = results.get('next_token')
            else:
                next_token = ""
        current_year += 1


@retry(wait_random_min=W_R_MIN, wait_random_max=W_R_MAX, stop_max_attempt_number=S_M_A_N)
def get_tweets_full_archive(string_query, param_start_time, param_end_time, param_next_token=""):
    """ Function to retrieve the tweets according to a query between two dates

    :param string_query: String with the query to look for, example: #WorldMentalHealthDay2021 -is:retweet lang:en
    :param param_start_time: Date from you're looking for tweets, UTC timestamp, example: 2019-09-10T00:00:00Z
    :param param_end_time: Date to you're looking for tweets, UTC timestamp, example: 2021-11-10T23:59:59Z
    :param param_next_token: This parameter is used to get the next page of results. The value used with the parameter
                             is pulled directly from the response provided by the API, and should not be modified.
    :return: json with the list of start dates and end dates and tweets registered, and next_token if exists
    Example of return:
    {
    "data": [
        {
            "source": "Twitter for Android",
            "public_metrics": {
                "retweet_count": 0,
                "reply_count": 0,
                "like_count": 0,
                "quote_count": 0
            },
            "possibly_sensitive": false,
            "author_id": "253354299",
            "text": "Today Is #WorldMentalHealthDay",
            "id": "1447268185993064460",
            "conversation_id": "1447268185993064460",
            "lang": "en",
            "reply_settings": "everyone",
            "created_at": "2021-10-10T18:29:59.000Z"
        },
        {
            "source": "Twitter for iPhone",
            "public_metrics": {
                "retweet_count": 0,
                "reply_count": 0,
                "like_count": 1,
                "quote_count": 0
            },
            "possibly_sensitive": false,
            "author_id": "957590648755924992",
            "text": "Hey @jarpad … I know you’re a big supporter of #Mentalhealth … can I ...",
            "referenced_tweets": [
                {
                    "type": "quoted",
                    "id": "1447217512160976897"
                }
            ],
            "id": "1447268184373948417",
            "conversation_id": "1447268184373948417",
            "lang": "en",
            "reply_settings": "everyone",
            "created_at": "2021-10-10T18:29:58.000Z"
        }
        ],
    "includes": {
        "users": [
            {
                "public_metrics": {
                    "followers_count": 842,
                    "following_count": 1521,
                    "tweet_count": 5549,
                    "listed_count": 5
                },
                "created_at": "2011-02-17T02:21:05.000Z",
                "name": "•.¸¸.•*¨¨*•.¸¸. 𝗡 𝗘 𝗙 𝗧 𝗔 𝗟 𝗧 𝗔 𝗜 𝗥 ",
                "protected": false,
                "id": "253354299",
                "verified": false,
                "url": "https://t.co/UBUhGafjlu",
                "description": "🇲🇽OAX CPE Ing. En Alimentos @soulcalibur  May 17TH 1990  ...",
                "username": "Altair_Neftali",
                "location": "Campeche, México"
            },
            {
                "public_metrics": {
                    "followers_count": 2330,
                    "following_count": 1471,
                    "tweet_count": 26225,
                    "listed_count": 27
                },
                "created_at": "2018-01-28T12:26:16.000Z",
                "name": "Changing Channels",
                "protected": false,
                "id": "957590648755924992",
                "verified": false,
                "url": "https://t.co/K9UOMEFwpx",
                "description": "Covering the Winchesters, ...",
                "username": "ChangingChanne1"
            }
        ]
    },
    "meta": {
        "newest_id": "1447268185993064460",
        "oldest_id": "1447267764809445380",
        "result_count": 99,
        "next_token": "b26v89c19zqg8o3fpds8ylg0bc4oz8yro0dlzss8f9ny5"
        }
    }

    """

    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Bearer " + token
    url = base_url + 'search/all'
    print(url)
    params = dict()
    params['query'] = string_query
    params['start_time'] = param_start_time
    params['end_time'] = param_end_time
    params['max_results'] = 100
    params['tweet.fields'] = "id,text,author_id,conversation_id,created_at,geo,in_reply_to_user_id,lang," \
                             "public_metrics,referenced_tweets,reply_settings,source,possibly_sensitive"
    params['expansions'] = "author_id"
    params['user.fields'] = "id,created_at,description,location,name,protected,public_metrics,url,username,verified"
    if param_next_token != "":
        params['next_token'] = param_next_token
    response = requests.get(url,
                            params=params,
                            headers=headers)
    return response.text


def process_wmhd_tweet_years(query, start_time, end_time, next_token, mode_test=True):
    """ Function to get content and authors of tweets related to World Mental Health Day
        from year_start to year_end, on a 2 month period per year from sept-10th to nov-10th.
        WMHD is on oct-10th every year.

    :param query: String with the query to look for, example: #WorldMentalHealthDay2021 -is:retweet lang:en
    :param start_time: Date from you're looking for tweets, UTC timestamp, example: 2019-09-10T00:00:00Z
    :param end_time: Date to you're looking for tweets, UTC timestamp, example: 2021-11-10T23:59:59Z
    :param next_token: This parameter is used to get the next page of results. The value used with the parameter
                       is pulled directly from the response provided by the API, and should not be modified.
    :param mode_test: Optional parameter Boolean to determine if using test or final db. Value Default: True -> TestDB
    :return: returned next_token or ""
    """
    db_instance = DBAccessHelper.DBAccessHelper(mode_test)
    str_search = get_tweets_full_archive(query, start_time, end_time, next_token)
    json_search = json.loads(str_search)
    tweets = json_search.get('data')
    if tweets is not None:
        db_instance.insert_tweets(tweets)
    if json_search.get('includes') is not None:
        authors = json_search.get('includes').get('users')
        if authors is not None:
            db_instance.insert_authors(authors)
    results = json_search.get('meta')
    if results is not None:
        if results.get('next_token') is not None:
            next_token = results.get('next_token')
        else:
            next_token = ""
    else:
        next_token = ""
    return next_token


def get_wmhd_tweets_years(year_start=2019, year_end=2021, db_test=True, init_token=""):
    """ Function to get content and authors of tweets related to World Mental Health Day
        from year_start to year_end, on a 2 month period per year from sept-10th to nov-10th.
        WMHD is on oct-10th every year.

    :param year_start: Optional parameter Integer value to begin the search. Value Default: 2019
    :param year_end: Optional parameter Integer value to finish the search. Value Default: 2021
    :param db_test: Optional parameter Boolean to determine if using test or final db. Value Default: True -> TestDB
    :param init_token: Optional parameter String to continue previous search. Value Default: "" -> Start by first call
    :return:
    """

    current_year = year_start
    while current_year <= year_end:
        year = str(current_year)
        query = "#WorldMentalHealthDay" + year + " OR #WorldMentalHealthDay -is:retweet lang:en"
        start_time = year + "-09-10T00:00:00Z"
        end_time = year + "-11-10T23:59:59Z"
        next_token = process_wmhd_tweet_years(query, start_time, end_time, init_token, db_test)
        while next_token != "":
            print("Next token: {}".format(str(next_token)))
            next_token = process_wmhd_tweet_years(query, start_time, end_time, next_token, db_test)
        current_year += 1


if __name__ == '__main__':

    get_wmhd_tweets_years(2019, 2021, True, "")

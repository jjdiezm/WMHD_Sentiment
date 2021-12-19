# WMHD_Sentiment
Python study of tweets related with World Mental Health Day 2021  
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Anaconda 3](https://anaconda.org/conda-forge/python/badges/version.svg)](https://docs.anaconda.com/anaconda/reference/release-notes/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

This Python aplication consists on six files:

1.	**DBAccessHelper.py** Python class with MS Access read and write operations. Some text processing function are mixed with it to optimize DB commits.
2.	**auth_twitter.py** Personal credentials to access Twitter API, it's filled with ****************** that must be changed by real credentials.
3.	**twitter_api.py** Pyhton Functions to query the Twitter API to download tweets, there are some functions to count number of tweets related to known the volumes prior to download.
4.	**tweet_process.py** Funtion to simplify the tweets' text, token them or transform emojis like :smile: into its names like `:smile:`.
5.	**sentiment.py** Sentiment analysis functions. Based on TextBlob anb Vader. Includes reporting and graphic generation.
6.	**main.py** Console menu to handle the functions and simplify the aplication use.

The application was built for my Final Master Project at [Universitat Oberta de Catalunya - UOC](https://www.uoc.edu).  
Use it at your convenience but please respect the open source license [GPL v3](https://github.com/jjdiezm/WMHD_Sentiment/blob/main/LICENSE).


#!/usr/bin/python
# -*- coding: utf-8 -*-
# Coded by jdiezm@uoc.edu to perform 2021 TFM on WMHD Sentiment Analysis

import os
import DBAccessHelper
import twitter_api
import sentiment


def clear_screen():
    """ Function to avoid OS issues on cleaning screen

    :return: Nothing
    """
    if os.name == "nt":
        os.system('cls')
    else:
        os.system('clear')


def menu(db_test=True):
    """  Function to clean screen and manage the menu

    :param db_test: True to use database on mode Test, False for main database
    :return:
    """
    clear_screen()
    if db_test:
        database_mode = "Test Database"
    else:
        database_mode = "Main Database"
    print("Please select one option: ")
    print("\t1 - Test database (print tables names on db)")
    print("\t2 - Download WMHD Tweets from API")
    print("\t3 - Clean Tweets text to perform the sentiment analysis on it -MANDATORY TO PERFORM ANALYSIS-")
    print("\t4 - Perform Vader calculations for sentiment analysis")
    print("\t5 - Perform TextBlob calculations for sentiment analysis")
    print("\t6 - Reporting and Graphics from the database")
    print("\t7 - Change database to work on. Current: " + database_mode)
    print("\t0 - Exit")


if __name__ == '__main__':
    db_mode_test = True
    while True:
        menu(db_mode_test)
        option_menu = input("Please input a number + [ENTER] to run an option >> ")
        if option_menu == "1":
            clear_screen()
            print("TESTING DB CONNECTION")
            print("*********************")
            if db_mode_test:
                print("Data base connection tested: Test Database")
            else:
                print("Data base connection tested: Main Database")
            db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
            db_instance.db_tables()
        elif option_menu == "2":
            clear_screen()
            print("GET TWEETS FROM API AND SAVE TO DB")
            print("**********************************")
            print("Warning! This operation WILL TAKE A VERY LONG TIME")
            print("Getting content and authors of tweets related to World Mental Health Day from [year_start] "
                  "to [year_end], on a 2 month period per year from sept-10th to nov-10th. "
                  "WMHD is on oct-10th every year. Can be continued from a previous search using a [token]")
            year_start = input("Please type the year + [ENTER] to start e.g.: 2019 >> ")
            year_end = input("Please type the year + [ENTER] to end e.g.: 2021 >> ")
            token = input("Please type a token + [ENTER] to continue previous or [ENTER] only to start from 0 >> ")
            if token == "":
                print(f"You will proceed on getting tweets from {year_start} to {year_end} without token {token}")
            else:
                print(f"You will proceed on getting tweets from {year_start} to {year_end} with token: {token}")
            proceed = input("Do you want to proceed? [y] + [enter] to proceed any other + [enter] to abort >> ")
            if proceed == "y":
                twitter_api.get_wmhd_tweets_years(int(year_start), int(year_end), db_mode_test, token)
            else:
                print("Get tweets from API aborted")
        elif option_menu == "3":
            clear_screen()
            print("CLEANED TEXT AND REDUCED TEXT GENERATION AND UPDATE ON DB")
            print("*********************************************************")
            print("Warning! This operation may take a long time, please select an option: ")
            print("\t1 - Update only on Tweets without cleaned text field")
            print("\t2 - Regenerate all the cleaned text on ALL THE TWEETS (May take long time)")
            print("\t3 - Update only on Tweets without reduced text field - Needed for wordcount method")
            print("\t4 - Regenerate all the reduced text on ALL THE TWEETS (May take long time)")
            print("\t5 - Do Nothing and go back to main menu")
            response2 = input("Please input another number + [ENTER] to proceed >> ")
            if response2 == "1":
                db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
                db_instance.update_tweets_cleaned_text(update_existent=False)
            elif response2 == "2":
                db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
                db_instance.update_tweets_cleaned_text(update_existent=True)
            elif response2 == "3":
                db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
                db_instance.update_tweets_text_reduced(update_existent=False)
            elif response2 == "4":
                db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
                db_instance.update_tweets_text_reduced(update_existent=True)
            else:
                print("Finishing without processing")
        elif option_menu == "4":
            clear_screen()
            print("VADER SENTIMENT ANALYSIS AND UPDATE ON DB")
            print("*****************************************")
            print("Warning! This operation may take a long time, please select an option: ")
            print("\t1 - Update only registers on DB without Vader Data")
            print("\t2 - Regenerate all the Vader Data on ALL THE TWEETS (May take long time)")
            print("\t3 - Do Nothing and go back to main menu")
            response2 = input("Please input another number + [ENTER] to proceed >> ")
            if response2 == "1":
                db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
                db_instance.vader_calculations(update_existent=False)
            elif response2 == "2":
                db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
                db_instance.vader_calculations(update_existent=True)
            else:
                print("Finishing without processing")
        elif option_menu == "5":
            clear_screen()
            print("TEXTBLOB SENTIMENT ANALYSIS AND UPDATE ON DB")
            print("********************************************")
            print("Warning! This operation may take a long time, please select an option: ")
            print("\t1 - Update only registers on DB without TextBlob Data")
            print("\t2 - Regenerate all the TextBlob Data on ALL THE TWEETS (May take long time)")
            print("\t3 - Do Nothing and go back to main menu")
            response2 = input("Please input another number + [ENTER] to proceed >> ")
            if response2 == "1":
                db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
                db_instance.textblob_calculations(update_existent=False)
            elif response2 == "2":
                db_instance = DBAccessHelper.DBAccessHelper(mode_test=db_mode_test)
                db_instance.textblob_calculations(update_existent=True)
            else:
                print("Finishing without processing")
        elif option_menu == "6":
            clear_screen()
            print("GENERAL DATA AND SENTIMENT ANALYSIS REPORTING")
            print("*********************************************")
            print("\t1 - Print general information")
            print("\t2 - Graphics on general information")
            print("\t3 - Print sentiment analysis information")
            print("\t4 - Generate Vader WordClouds")
            print("\t5 - Generate TextBlob WordClouds")
            print("\t6 - Generate Most Common Words Reports based on Vader Sentiment")
            print("\t7 - Generate Most Common Words Reports based on TextBlob Sentiment")
            print("\t8 - Do Nothing and go back to main menu")
            response2 = input("Please input another number + [ENTER] to proceed >> ")
            if response2 == "1":
                clear_screen()
                print("GENERAL INFORMATION")
                print("*******************")
                print("\t1 - Print general information no filtering")
                print("\t2 - Print general information from relevant users (certified and more than 5K followers)")
                print("\t3 - Print general information from relevant tweets (more than 500 retweets)")
                print("\t4 - Print general information from liked tweets (more than 500 likes)")
                print("\t5 - Print general information from relevant tweets from relevant users")
                print("\t6 - Print general information from liked tweets from relevant users")
                print("\t7 - Do Nothing and go back to main menu")
                response3 = input("Please input another number + [ENTER] to proceed >> ")
                if response3 == "1":
                    sentiment.print_general(param_account="all", param_tweet="all")
                elif response3 == "2":
                    sentiment.print_general(param_account="certified", param_tweet="all")
                elif response3 == "3":
                    sentiment.print_general(param_account="all", param_tweet="relevant")
                elif response3 == "4":
                    sentiment.print_general(param_account="all", param_tweet="liked")
                elif response3 == "5":
                    sentiment.print_general(param_account="certified", param_tweet="relevant")
                elif response3 == "6":
                    sentiment.print_general(param_account="certified", param_tweet="liked")
            elif response2 == "2":
                print("PLEASE WAIT FOR A GRAPHICAL WINDOW")
                print("-Pay attention because can raise without caption-")
                sentiment.graphic_general()
            elif response2 == "3":
                clear_screen()
                print("SENTIMENT ANALYSIS INFORMATION")
                print("******************************")
                print("\t1 - Print sentiment information no filtering")
                print("\t2 - Print sentiment information from relevant users (certified and more than 5K followers)")
                print("\t3 - Print sentiment information from relevant tweets (more than 500 retweets)")
                print("\t4 - Print sentiment information from liked tweets (more than 500 likes)")
                print("\t5 - Print sentiment information from relevant tweets from relevant users")
                print("\t6 - Print sentiment information from liked tweets from relevant users")
                print("\t7 - Do Nothing and go back to main menu")
                response3 = input("Please input another number + [ENTER] to proceed >> ")
                if response3 == "1":
                    sentiment.print_sentiment(param_account="all", param_tweet="all")
                elif response3 == "2":
                    sentiment.print_sentiment(param_account="certified", param_tweet="all")
                elif response3 == "3":
                    sentiment.print_sentiment(param_account="all", param_tweet="relevant")
                elif response3 == "4":
                    sentiment.print_sentiment(param_account="all", param_tweet="liked")
                elif response3 == "5":
                    sentiment.print_sentiment(param_account="certified", param_tweet="relevant")
                elif response3 == "6":
                    sentiment.print_sentiment(param_account="certified", param_tweet="liked")
            elif response2 == "4":
                sentiment.generate_main_cloud("all", "Vader")
            elif response2 == "5":
                sentiment.generate_main_cloud("all", "TextBlob")
            elif response2 == "6":
                clear_screen()
                for filter_sentiment in ["all", "positive", "negative", "neutral"]:
                    sentiment.most_common_words(filter_sentiment, "Vader")
            elif response2 == "7":
                clear_screen()
                for filter_sentiment in ["all", "positive", "negative", "neutral"]:
                    sentiment.most_common_words(filter_sentiment, "TextBlob")
            else:
                print("Finishing without processing")
        elif option_menu == "8":
            clear_screen()
            db_mode_test = not db_mode_test
            if db_mode_test:
                print("Data base changed to Test Database")
            else:
                print("Data base changed to Main Database")
        elif option_menu == "0":
            clear_screen()
            print("Application closed...")
            break
        else:
            print("\nUnexpected option...")
        input(f"\nAction finished...\n\nPlease press a key to continue to main menu")

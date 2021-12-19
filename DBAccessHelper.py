#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Coded by jdiezm@uoc.edu to perform 2021 TFM on WMHD Sentiment Analysis

from os import getcwd
import pyodbc
import tweet_process
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob


class DBAccessHelper(object):
    """ Class to manage tweets_db.accdb Access Database."""

    def __init__(self, mode_test=True):
        """ Init method to setup ODBC connection parameters
            To create objects like dbHelperMainDB = DBAccessHelper(True) or dbHelperTestDB = DBAccessHelper(False)
            :param mode_test: if mode_test is set to True (default) uses test DB else Main DB
        """
        self.driver_name = "Microsoft Access Driver (*.mdb, *.accdb)"
        if mode_test:
            self.db_path = getcwd() + "/tweets_db_test.accdb"
        else:
            self.db_path = getcwd() + "/tweets_db.accdb"

    @staticmethod
    def remove_special_characters(character):
        """ Static method to process a character and return True if on special_chars list.
            Example of use: sql_update_clean = ''.join(filter(self.remove_special_characters, sql_update))
        :param character: character to compare with the list
        :return: True or False
        """
        special_chars = ['@', '#', "'"]
        if character not in special_chars:
            return True
        else:
            return False

    def insert_authors(self, data):
        """ Method to insert/update the authors on the Tweet Database as the Twitter's API includes.users response

        :param data: JSON with the includes.users Twitter API
        :return: Nothing
        """
        inserted = 0
        updated = 0
        insert_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        insert_cursor = insert_conn.cursor()

        pre_insert = u"INSERT INTO tbl_Authors (id, verified, shown_name, description, created_at, protected, url, " \
                     u"username, location, followers_count, following_count, tweet_count, listed_count) " \
                     u"VALUES ('{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}', {}, {}, {}, {})"

        pre_update = u"UPDATE tbl_Authors set verified = {}, shown_name = '{}', description = '{}', " \
                     u"created_at = '{}', protected = {}, url = '{}', username = '{}', location = '{}', " \
                     u"followers_count = {}, following_count = {}, tweet_count = {}, listed_count = {} " \
                     u"WHERE id = '{}'"

        for user in data:
            current_id = user.get("id")
            current_verified = user.get("verified")
            if current_verified is None:
                current_verified = False
            current_name = user.get("name")
            if current_name is None:
                current_name = ""
            else:
                current_name = current_name.replace("'", "´")
            current_description = user.get("description")
            if current_description is None:
                current_description = ""
            else:
                current_description = current_description.replace("'", "´")
            current_created_at = user.get("created_at")
            if current_created_at is None:
                current_created_at = ""
            current_protected = user.get("protected")
            if current_protected is None:
                current_protected = False
            current_url = user.get("url")
            if current_url is None:
                current_url = ""
            current_username = user.get("username")
            if current_username is None:
                current_username = ""
            else:
                current_username = current_username.replace("'", "´")
            current_location = user.get("location")
            if current_location is None:
                current_location = ""
            else:
                current_location = current_location.replace("'", "´")
            current_followers_count = user.get("public_metrics").get("followers_count")
            if current_followers_count is None:
                current_followers_count = 0
            current_following_count = user.get("public_metrics").get("following_count")
            if current_following_count is None:
                current_following_count = 0
            current_tweet_count = user.get("public_metrics").get("tweet_count")
            if current_tweet_count is None:
                current_tweet_count = 0
            current_listed_count = user.get("public_metrics").get("listed_count")
            if current_listed_count is None:
                current_listed_count = 0

            sql_update = pre_update.format(current_verified, current_name, current_description,
                                           current_created_at, current_protected, current_url, current_username,
                                           current_location, current_followers_count, current_following_count,
                                           current_tweet_count, current_listed_count, current_id)
            sql_insert = pre_insert.format(current_id, current_verified, current_name, current_description,
                                           current_created_at, current_protected, current_url, current_username,
                                           current_location, current_followers_count, current_following_count,
                                           current_tweet_count, current_listed_count)
            # sql_update_clean = ''.join(filter(self.remove_special_characters, sql_update))
            # sql_insert_clean = ''.join(filter(self.remove_special_characters, sql_insert))
            update_result = insert_cursor.execute(sql_update)
            if update_result.rowcount == 0:
                insert_cursor.execute(sql_insert)
                inserted += 1
                insert_conn.commit()
            else:
                updated += 1
                insert_conn.commit()
        insert_cursor.close()
        insert_conn.close()
        print("Inserted: {} authors".format(str(inserted)))
        print("Updated: {} authors".format(str(updated)))

    def db_tables(self):
        """ Method to print tables defined on the DB

        :return: Nothing
        """

        tables_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        tables_cursor = tables_conn.cursor()
        for table_info in tables_cursor.tables(tableType='TABLE'):
            print(table_info.table_name)
        tables_cursor.close()
        tables_conn.close()

    def get_author(self, user_id):
        """ Method to get the Author info of a determined user_id

        :param user_id: Id of the Author to get the info
        :return: rows list as result of the query
        """
        user_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        user_cursor = user_conn.cursor()
        qry = "SELECT * FROM tbl_Authors WHERE Id = '{}'".format(str(user_id))
        results = user_cursor.execute(qry)
        rows_qry = results.fetchall()
        user_cursor.close()
        user_conn.close()
        return rows_qry

    def print_author(self, user_id):
        """ Method to print the Author info of a determined user_id

        :param user_id: Id of the Author to get the info
        :return: Nothing
        """

        rows_result = self.get_author(user_id)
        if rows_result:
            for row_result in rows_result:
                print(row_result)
        else:
            print("No data from user {} found on Authors Table.".format(str(user_id)))

    def insert_referenced(self, data):
        """ Method to insert/update the referenced Tweets on the Tweet Database
            as the Twitter's API referenced_tweets response

        :param data: JSON with the referenced_tweets returned on get Tweet from Twitter API
        :return: Number of referenced tweets inserted or updated at DB
        """
        inserted = 0
        updated = 0
        insert_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        insert_cursor = insert_conn.cursor()

        pre_insert = u"INSERT INTO tbl_Referenced (original, referenced, type) " \
                     u"VALUES ('{}', '{}', '{}')"

        pre_update = u"UPDATE tbl_Referenced set type = '{}' " \
                     u"WHERE (original = '{}' and referenced = '{}')"

        current_original = data.get("id")
        for reference in data.get("referenced_tweets"):
            current_referenced = reference.get("id")
            current_type = reference.get("type")

            sql_update = pre_update.format(current_type, current_original, current_referenced)
            sql_insert = pre_insert.format(current_original, current_referenced, current_type)
            update_result = insert_cursor.execute(sql_update)
            if update_result.rowcount == 0:
                insert_cursor.execute(sql_insert)
                inserted += 1
                insert_conn.commit()
            else:
                updated += 1
                insert_conn.commit()
        insert_cursor.close()
        insert_conn.close()
        # print("Inserted: {} referenced Tweets".format(str(inserted)))
        # print("Updated: {} referenced Tweets".format(str(updated)))
        return inserted + updated

    def insert_tweets(self, data):
        """ Method to insert/update the Tweets on the Tweet Database as the Twitter's API get Tweet response

        :param data: JSON with the tweets returned by Twitter API
        :return: Nothing
        """
        inserted = 0
        updated = 0
        insert_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        insert_cursor = insert_conn.cursor()

        pre_insert = u"INSERT INTO tbl_Tweets (id, fk_author, created_at, original_text, in_reply_to_user," \
                     u"reply_settings, conversation_id, source, lang, retweet_count, reply_count, like_count," \
                     u"quote_count, possibly_sensitive, referenced_tweets_count) " \
                     u"VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {})"

        pre_update = u"UPDATE tbl_Tweets set fk_author = '{}', created_at = '{}', original_text = '{}', " \
                     u"in_reply_to_user = '{}', reply_settings = '{}', conversation_id = '{}', source = '{}', " \
                     u"lang = '{}', retweet_count = {}, reply_count = {}, like_count = {}, quote_count = {}, " \
                     u"possibly_sensitive = {}, referenced_tweets_count = {} WHERE id = '{}'"

        current_created_at = ""
        for tweet in data:
            current_id = tweet.get("id")
            current_fk_author = tweet.get("author_id")
            if current_fk_author is None:
                current_fk_author = ""
            current_created_at = tweet.get("created_at")
            if current_created_at is None:
                current_created_at = ""
            else:
                current_created_at = current_created_at
            current_original_text = tweet.get("text")
            if current_original_text is None:
                current_original_text = ""
            else:
                current_original_text = current_original_text.replace("'", "´")
            current_in_reply_to_user = tweet.get("in_reply_to_user_id")
            if current_in_reply_to_user is None:
                current_in_reply_to_user = ""
            current_reply_settings = tweet.get("reply_settings")
            if current_reply_settings is None:
                current_reply_settings = ""
            current_conversation_id = tweet.get("conversation_id")
            if current_conversation_id is None:
                current_conversation_id = ""
            current_source = tweet.get("source")
            if current_source is None:
                current_source = ""
            else:
                current_source = current_source.replace("'", "´")
            current_lang = tweet.get("lang")
            if current_lang is None:
                current_lang = ""
            current_retweet_count = tweet.get("public_metrics").get("retweet_count")
            if current_retweet_count is None:
                current_retweet_count = 0
            current_reply_count = tweet.get("public_metrics").get("reply_count")
            if current_reply_count is None:
                current_reply_count = 0
            current_like_count = tweet.get("public_metrics").get("like_count")
            if current_like_count is None:
                current_like_count = 0
            current_quote_count = tweet.get("public_metrics").get("quote_count")
            if current_quote_count is None:
                current_quote_count = 0
            current_possibly_sensitive = tweet.get("possibly_sensitive")
            if current_possibly_sensitive is None:
                current_possibly_sensitive = False
            if tweet.get("referenced_tweets") is not None:
                referenced_json = dict()
                referenced_json['id'] = tweet.get("id")
                referenced_json['referenced_tweets'] = tweet.get("referenced_tweets")
                current_referenced = self.insert_referenced(referenced_json)
            else:
                current_referenced = 0
            sql_update = pre_update.format(current_fk_author, current_created_at, current_original_text,
                                           current_in_reply_to_user, current_reply_settings, current_conversation_id,
                                           current_source, current_lang, current_retweet_count, current_reply_count,
                                           current_like_count, current_quote_count, current_possibly_sensitive,
                                           current_referenced, current_id)
            sql_insert = pre_insert.format(current_id, current_fk_author, current_created_at, current_original_text,
                                           current_in_reply_to_user, current_reply_settings, current_conversation_id,
                                           current_source, current_lang, current_retweet_count, current_reply_count,
                                           current_like_count, current_quote_count, current_possibly_sensitive,
                                           current_referenced)
            # sql_update_clean = ''.join(filter(self.remove_special_characters, sql_update))
            # sql_insert_clean = ''.join(filter(self.remove_special_characters, sql_insert))
            update_result = insert_cursor.execute(sql_update)
            if update_result.rowcount == 0:
                insert_cursor.execute(sql_insert)
                inserted += 1
                insert_conn.commit()
            else:
                updated += 1
                insert_conn.commit()
        insert_cursor.close()
        insert_conn.close()
        print("Inserted: {} tweets".format(str(inserted)))
        print("Updated: {} tweets".format(str(updated)))
        if current_created_at != "":
            print("Last tweet created at: {}".format(str(current_created_at)))

    def get_all_tweets(self):
        """ Method to get all the Tweets registered on the Tweet Database

        :return:  rows list as result of the query
        """
        user_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        user_cursor = user_conn.cursor()
        qry = "SELECT * FROM tbl_Tweets"
        results = user_cursor.execute(qry)
        rows_qry = results.fetchall()
        user_cursor.close()
        user_conn.close()
        return rows_qry

    def get_tweets_not_cleaned(self):
        """ Method to get all the Tweets with the cleaned_text field null registered on the Tweet Database

        :return:  rows list as result of the query
        """
        user_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        user_cursor = user_conn.cursor()
        qry = "SELECT * FROM tbl_Tweets WHERE cleaned_text Is Null"
        results = user_cursor.execute(qry)
        rows_qry = results.fetchall()
        user_cursor.close()
        user_conn.close()
        return rows_qry

    def update_tweets_cleaned_text(self, update_existent=False):
        """ Function to generate the cleaned text from original Tweet text and store on database's field [cleaned_text]

        :param update_existent: If set to False (default) Only process database register with null cleaned_text field
                                If set to True recalculates and rewrites all Tweets field cleaned_text.
        :return: Nothing
        """
        update_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        update_cursor = update_conn.cursor()
        pre_update = u"UPDATE tbl_Tweets set cleaned_text = '{}' WHERE id = '{}'"
        total = 0
        subtotal = 0
        if update_existent:
            rows_result = self.get_all_tweets()
        else:
            rows_result = self.get_tweets_not_cleaned()
        for row_result in rows_result:
            total += 1
            sql_update = pre_update.format(tweet_process.normalize_tweet(row_result[3]), row_result[0])
            update_result = update_cursor.execute(sql_update)
            if subtotal >= 1000:
                update_cursor.commit()
                subtotal = 0
                print("Total Updated: {}".format(str(total)))
            else:
                subtotal += 1
        print("Total Updated: {}".format(str(total)))

        update_cursor.commit()
        update_cursor.close()
        update_conn.close()

    def query_to_df(self, query):
        """ Method to query the Database and return as Pandas DataFrame

        :param query: String with the query to return as Pandas df.
                      Example: "select * from tbl_Tweets"
        :return: pandas dataframe with the result of the query
        """
        query_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        dataframe = pd.read_sql(query, query_conn)
        return dataframe

    def df_to_odbc(self, df, table_name):
        """

        :param df:
        :param table_name:
        :return: Nothing
        """
        # https://github.com/mkleehammer/pyodbc/blob/master/tests3/accesstests.py
        # https://www.cdata.com/kb/tech/access-python-pandas.rst
        # DE DF a ODBC

        create_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        create_cursor = create_conn.cursor()

        df_tuple_columns = tuple(list(df.columns))
        aux_columns = len(list(df.columns)) - 1
        values_text = '(?' + ', ?' * aux_columns + ')'  # (?, ?)
        create_cursor.executemany(f"INSERT INTO [{table_name}] {df_tuple_columns} VALUES {values_text}",
                                  df.itertuples(index=False))

    def df_to_update_field(self, df, df_id, field_name, table_name):
        """ Method to update a field on a table with the column values of a pandas dataframe
        :param df: Pandas data frame with the information to update, must be field, id
        :param df_id: Name on the DataFrame with the table id  (must be the same on df and table)
        :param field_name: Name of the field to update (must be the same on df and table)
        :param table_name: The Table to Update
        :return: Nothing
        """
        create_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        create_cursor = create_conn.cursor()

        create_cursor.executemany(f"UPDATE {table_name} set {field_name} = '?' WHERE {df_id} = '?'",
                                  df.itertuples(index=False))

    def get_tweets_not_vader(self):
        """ Method to get all the Tweets with the vader_sentiment field null registered on the Tweet Database

        :return:  rows list as result of the query
        """
        user_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        user_cursor = user_conn.cursor()
        qry = "SELECT * FROM qry_Tweets_EN WHERE vader_sentiment Is Null"
        results = user_cursor.execute(qry)
        rows_qry = results.fetchall()
        user_cursor.close()
        user_conn.close()
        return rows_qry

    def vader_calculations(self, update_existent=False):
        """ Method to generate the Vader sentiment analysis and store on Database fields

        :param update_existent: If set to False (default) Only process database register with null vader data
                                If set to True recalculates and rewrites all Tweets vader data.
        :return: Nothing
        """
        update_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        update_cursor = update_conn.cursor()
        nltk.download('vader_lexicon')
        pre_update = u"UPDATE tbl_Tweets set vader_sentiment = '{}', vader_neg = '{}', vader_neu = '{}', " \
                     u"vader_pos = '{}', vader_compound = '{}' WHERE id = '{}'"
        total = 0
        subtotal = 0
        if update_existent:
            rows_result = self.get_all_tweets()
        else:
            rows_result = self.get_tweets_not_vader()
        for row_result in rows_result:
            total += 1
            vader_score = SentimentIntensityAnalyzer().polarity_scores(row_result[14])
            if vader_score['neg'] > vader_score['pos']:
                aux_temp = "negative"
            elif vader_score['pos'] > vader_score['neg']:
                aux_temp = "positive"
            else:
                aux_temp = "neutral"
            sql_update = pre_update.format(aux_temp, vader_score['neg'], vader_score['neu'], vader_score['pos'],
                                           vader_score['compound'], row_result[0])
            update_result = update_cursor.execute(sql_update)
            if subtotal >= 100:
                update_cursor.commit()
                subtotal = 0
                print("Total Updated: {}".format(str(total)))
            else:
                subtotal += 1
        print("Total Updated: {}".format(str(total)))

        update_cursor.commit()
        update_cursor.close()
        update_conn.close()

    def get_tweets_not_textblob(self):
        """ Method to get all the Tweets with the textblob_sentiment field null registered on the Tweet Database

        :return:  rows list as result of the query
        """
        user_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        user_cursor = user_conn.cursor()
        qry = "SELECT * FROM qry_Tweets_EN WHERE textblob_sentiment Is Null"
        results = user_cursor.execute(qry)
        rows_qry = results.fetchall()
        user_cursor.close()
        user_conn.close()
        return rows_qry

    def textblob_calculations(self, update_existent=False):
        """ Method to generate the sentiment analysis based on Textblob method and store on Database fields

        :param update_existent: If set to False (default) Only process database register with null Textblob data
                                If set to True recalculates and rewrites all Tweets Textblob data.
        :return: Nothing
        """
        update_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        update_cursor = update_conn.cursor()
        pre_update = u"UPDATE tbl_Tweets set textblob_sentiment = '{}', textblob_polarity = '{}' WHERE id = '{}'"
        total = 0
        subtotal = 0
        if update_existent:
            rows_result = self.get_all_tweets()
        else:
            rows_result = self.get_tweets_not_textblob()
        for row_result in rows_result:
            total += 1
            analysis = TextBlob(row_result[14])
            textblob_polarity = analysis.sentiment.polarity
            if textblob_polarity < 0:
                aux_temp = "negative"
            elif textblob_polarity > 0:
                aux_temp = "positive"
            else:
                aux_temp = "neutral"
            sql_update = pre_update.format(aux_temp, textblob_polarity, row_result[0])
            update_result = update_cursor.execute(sql_update)
            if subtotal >= 100:
                update_cursor.commit()
                subtotal = 0
                print("Total Updated: {}".format(str(total)))
            else:
                subtotal += 1
        print("Total Updated: {}".format(str(total)))

        update_cursor.commit()
        update_cursor.close()
        update_conn.close()

    def get_tweets_not_reduced(self):
        """ Method to get all the Tweets with the cleaned_text field null registered on the Tweet Database

        :return:  rows list as result of the query
        """
        user_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        user_cursor = user_conn.cursor()
        qry = "SELECT * FROM tbl_Tweets WHERE text_reduced Is Null"
        results = user_cursor.execute(qry)
        rows_qry = results.fetchall()
        user_cursor.close()
        user_conn.close()
        return rows_qry

    def update_tweets_text_reduced(self, update_existent=False):
        """ Function to generate the [text_reduced] from database's field [cleaned_text]

        :param update_existent: If set to False (default) Only process database register with null cleaned_text field
                                If set to True recalculates and rewrites all Tweets field cleaned_text.
        :return: Nothing
        """
        update_conn = pyodbc.connect("Driver={%s};DBQ=%s;" % (self.driver_name, self.db_path))
        update_cursor = update_conn.cursor()
        pre_update = u"UPDATE tbl_Tweets set text_reduced = '{}' WHERE id = '{}'"
        total = 0
        subtotal = 0
        if update_existent:
            rows_result = self.get_all_tweets()
        else:
            rows_result = self.get_tweets_not_reduced()
        for row_result in rows_result:
            total += 1
            sql_update = pre_update.format(tweet_process.reduce_text(row_result[3]), row_result[0])
            update_result = update_cursor.execute(sql_update)
            if subtotal >= 1000:
                update_cursor.commit()
                subtotal = 0
                print("Total Updated: {}".format(str(total)))
            else:
                subtotal += 1
        print("Total Updated: {}".format(str(total)))

        update_cursor.commit()
        update_cursor.close()
        update_conn.close()

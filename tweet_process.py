# Coded by jdiezm@uoc.edu to perform 2021 TFM on WMHD Sentiment Analysis

from emoji import demojize
from nltk.tokenize import TweetTokenizer
import nltk
import re


tokenizer = TweetTokenizer()


def normalize_token(token):
    """ Function to receive a token (word) and change urls by 'URL', usernames by '@USER' and emojis by its description


    :param token: token (word) to normalize
    :return: token normalized
    """
    lower_cased_token = token.lower()
    if token.startswith("@"):
        return "@USER"
    elif lower_cased_token.startswith("http") or lower_cased_token.startswith("www"):
        return "URL"
    # elif token.startswith("#"):
    #     return "HASHTAG"
    elif len(token) == 1:
        return demojize(token)
    else:
        if token == "'":
            return "’"
        elif token == "…":
            return "..."
        elif token == "RT":
            return ""
        # elif token == '"':
        #     return ""
        else:
            return token


def normalize_tweet(tweet):
    """ Function to process tweets and generate the [cleaned_text] content by
            - applying normalize_token to all words
            - correct some commons misspellings

    :param tweet: tweet text
    :return: normalized text from original
    """
    tokens = tokenizer.tokenize(tweet.replace("'", "’").replace("…", "..."))
    norm_tweet = " ".join([normalize_token(token) for token in tokens])
    norm_tweet = (
        norm_tweet.replace("cannot ", "can not ")
                  .replace("n’t ", " n’t ")
                  .replace("n ’t ", " n’t ")
                  .replace("ca n’t", "can’t")
                  .replace("ai n’t", "ain’t")
       )
    norm_tweet = (
        norm_tweet.replace("’m ", " ’m ")
                  .replace("’re ", " ’re ")
                  .replace("’s ", " ’s ")
                  .replace("’ll ", " ’ll ")
                  .replace("’d ", " ’d ")
                  .replace("’ve ", " ’ve ")
       )
    norm_tweet = (
        norm_tweet.replace(" p . m .", "  p.m.")
                  .replace(" p . m ", " p.m ")
                  .replace(" a . m .", " a.m.")
                  .replace(" a . m ", " a.m ")
       )

    return " ".join(norm_tweet.split())


def tokenization(text):
    """ Function to tokenize text

    :param text: text to be tokenized
    :return: tokenized text
    """
    text = re.split('\W+', text)
    return text


def remove_stopwords(text):
    """ Function to remove English stop words, tech related words and main hashtags like #WorldMentalHealthDay

    :param text: text to be cleaned
    :return: cleaned text
    """
    stop_words = nltk.corpus.stopwords.words('english')
    for year in ["", "2019", "2020", "2021"]:
        stop_words.append("WorldMentalHealthDay".lower()+year)
    for word in ["on", "http", "https", "amp", "rt", "co", "url"]:
        stop_words.append(word)
    text = [word for word in text if word not in stop_words]
    return text


def stemming(text):
    """ Function to change words by its English root word

    :param text: text to be treated
    :return: treated text
    """
    ps = nltk.PorterStemmer()
    text = [ps.stem(word) for word in text]
    return text


def reduce_text(text):
    list_text = remove_stopwords(tokenization(text.lower()))
    return ' '.join([str(x) for x in list_text])


if __name__ == "__main__":
    example_tweet = "🗣NEWS: On #WorldMentalHealthDay #WorldMentalHealthDay2021 10 Oct in #London, we´re " \
                    "launching a powerful new exhibition " \
                    "in partnership with @HSBC exploring the dreams lost to neglected #mentalhealth &amp; #suicide, " \
                    "&amp; the hope that´s found through support. 👇 https://t.co/StdqqoQMxA #SpeakYourMind " \
                    "#40seconds https://t.co/r8LotSXPBY"
    print(example_tweet+"\n")
    print(normalize_tweet(example_tweet)+"\n")
    print(reduce_text(example_tweet)+"\n")

#!/usr/bin/env python

import re
import json


def remove_punctuation(string):
    """Remove all punctuation marks (except underscores) from a given string

    Keyword arguments:
    string -- string to be stripped from puncuation marks
    """
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*~'''

    # traverse the string and if any punctuation marks occur replace it with space
    for x in string:
        if x in punctuations:
            string = string.replace(x, " ")

    return string


def extract_hashtags(tweet):
    """Extract hashtags from a given tweet

    Keyword arguments:
    tweet -- tweet from dataset in JSON format
    """

    # List of all extracted hashtags
    hashtags_in_tweet = []

    # using field ['doc']['entities']['hashtags']
    for hashtag in tweet['doc']['entities']['hashtags']:

        # convert to lower-case string
        hashtag = hashtag['text'].lower()
        # remove punctuation and replace with space
        hashtag = remove_punctuation(hashtag)

        # if any spaces are present (after removing punctuation
        if re.search(r"\s", hashtag):
            print("There is a space present in the hashtag - using only first part of string")
            hashtag = hashtag.split(" ", 1)

        hashtags_in_tweet.append(hashtag)

    return hashtags_in_tweet


def extract_language(tweet):
    language = tweet['doc']['metadata']['iso_language_code']
    return language


def get_language(supported_languages, tweet_language_code):
    """Map a given ISO_639-1 language code to the long form language name

    Keyword arguments:
    supported_languages -- contents of the language_codes.json file
    """
    for language in supported_languages:
        if language['code'] == tweet_language_code:
            # if language code exists, return country name
            return language['name']
    return "Unknown"


def load_supported_languages(path_language_file):
    """Load the file containing Twitter's supported languages

    Keyword arguments:
    path_language_file -- path to the language file to load
    """
    with open(path_language_file, 'r') as language_file:
        supported_languages = json.load(language_file)
        return supported_languages


def print_results_hashtag_count(counter_hashtag):
    """Print the results from hashtag counting

    Keyword arguments:
    counter_hashtag -- counter with hashtag counts
    """
    i = 1
    print("Most common hashtags in dataset", flush=True)
    for hashtag in counter_hashtag.most_common(10):
        print(str(i) + ". #" + hashtag[0] + "," + str(hashtag[1]), flush=True)
        i += 1


def print_results_language_count(counter_language, supported_languages):
    """Print the results from language code counting.

    Keyword arguments:
    counter_language -- counter with language counts
    supported_languages -- contents of the language_codes.json file
    """
    j = 1
    print("Most common languages in dataset:", flush=True)
    for language in counter_language.most_common(10):
        print(str(j) + ". " + get_language(supported_languages, language[0]) +
              " (" + language[0] + ")" + ", " + str(language[1]), flush=True)
        j += 1

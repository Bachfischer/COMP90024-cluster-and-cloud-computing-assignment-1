#!/usr/bin/env python

import re


#TODO: replace with proper regex parsing
def remove_punctuation(string):
  # punctuation marks (underscore _ is considered valid)
  punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*~'''

  # traverse the given string and if any punctuation
  # marks occur replace it with space
  for x in string:
    if x in punctuations:
      string = string.replace(x, " ")

  return string


def extract_hashtags(tweets):
  # List of all extracted hashtags
  extracted_hashtags = []

  for tweet in tweets:
    # using field ['doc']['entities']['hashtags'] as per Richard's comments on LMS
    for hashtag in tweet['doc']['entities']['hashtags']:

      lower_hashtag = hashtag['text'].lower()
      #remove punctuation and replace with space
      lower_hashtag = remove_punctuation(lower_hashtag)

      # if any spaces are present (after removing punctuation
      if re.search(r"\s", lower_hashtag):
        print("There is a space present in the hashtag - using only first part of string")
        lower_hashtag = lower_hashtag.split(" ", 1)

      extracted_hashtags.append(lower_hashtag)

  return extracted_hashtags

def get_language(supported_languages, tweet_language_code):
  for language in supported_languages:
    if language['code'] == tweet_language_code:
      return language['name']
  return "Unknown"

#!/usr/bin/env python

import re
import json


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

# TODO: Error handling
def load_supported_languages(twitter_language_file):
  with open(twitter_language_file) as language_file:
    supported_languages = json.load(language_file)
    return supported_languages

# TODO: Error handling
def load_dataset(dataset_file):
  with open(dataset_file) as file:
    json_string = file.read()

    while True:
      try:
        # TODO: Fine more elegant solution for parsing corrupt JSON
        print("Trying to load JSON file")
        data = json.loads(json_string + "]}")  # adding missing brackets - expecting to add "]}"

      except Exception as e:
        print("Error loading JSON file - trying to fix corrupt data")
        json_string = json_string[:-1]  # Removing last character - expecting to remove ","
        continue
      break

  return data['rows']
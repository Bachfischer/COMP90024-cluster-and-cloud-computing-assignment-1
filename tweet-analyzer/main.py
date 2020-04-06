#!/usr/bin/env python

import json
import re
from pathlib import Path
from collections import Counter
import string
from utilities import get_language, extract_hashtags

with open("config.json") as config_file:
  config = json.load(config_file)


if config['production'] == True:
  home = str(Path.home())
  data_set = home + "/" + config['dataset']
else: # local development machine
  data_folder = Path("../data/")
  data_set = data_folder / config['dataset']

twitter_languages_file = config['supported_languages']

# Read supported Twitter languages from file
with open (twitter_languages_file) as language_file:
  supported_languages = json.load(language_file)

with open(data_set) as file:

  json_string = file.read()

  while True:

    try:
      # TODO: Fine more elegant solution for parsing corrupt JSON
      print("Trying to load JSON file")
      data = json.loads(json_string + "]}") # adding missing brackets - expecting to add "]}"

    except Exception as e:
      print("Error loading JSON file - trying to fix corrupt data")
      json_string = json_string[:-1] # Removing last character - expecting to remove ","
      continue
    break

tweets = data['rows']

# Extract hashtags from all tweets and add to list
extracted_hashtag = extract_hashtags(tweets)

# Counting hashtags
counter_hashtag = Counter(extracted_hashtag)

# Printing most common hashtags
i = 1
print("")
print("Most common hashtags in dataset")
for hashtag in counter_hashtag.most_common(10):
  print(str(i) + ". #" + hashtag[0] + "," + str(hashtag[1]))
  i +=1


# Couting tweet languages
counter_language = Counter(tweet['doc']['metadata']['iso_language_code'] for tweet in tweets)

# Printing most common tweet languages
j = 1
print("")
print("Most common languages in dataset:")
for language in counter_language.most_common(10):
  print(str(j) + ". " + get_language(supported_languages, language[0]) + " (" + language[0] + ")" + ", " + str(language[1]))
  j +=1

#!/usr/bin/env python

import json
import re
from pathlib import Path
from collections import Counter


def get_language(json_object, code):
  return [obj for obj in json_object if obj['code'] == code][0]['name']

twitter_languages_file = "languages_custom.json"

data_folder = Path("../data/")

data_set = data_folder / "tinyTwitter.json"


# Read supported Twitter languages from file
with open (twitter_languages_file) as language_file:
  languages = json.load(language_file)
  print(languages)

with open(data_set) as file:

  json_string = file.read()
  #print(json_string)

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

#print(data)

tweets = data['rows']

# Couting tweet languages
c = Counter(tweet['doc']['metadata']['iso_language_code'] for tweet in tweets)
top_languages = c.most_common(10)

for language in top_languages:
  print(get_language(languages, language[0]) + " - " + str(language[1]))

#for key, value in top_languages.items():
#  print(key)
#  print(get_language(languages, key) + " - " + str(value))


#!/usr/bin/env python

import json
import re
from pathlib import Path
from collections import Counter

data_folder = Path("../data/")

data_set = data_folder / "tinyTwitter.json"

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
print(c)
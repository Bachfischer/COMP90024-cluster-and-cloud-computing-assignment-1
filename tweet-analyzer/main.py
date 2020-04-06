#!/usr/bin/env python

import json
from pathlib import Path
from collections import Counter
from utilities import get_language, extract_hashtags, load_supported_languages, load_dataset, divide_list_into_chunks
from mpi4py import MPI
import numpy as np


def data_processing(tweets):
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


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

print("tweet-analyzer running on " + str(comm.size) + " cores")


# Read configuration options

with open("config.json") as config_file:
  config = json.load(config_file)

if config['production'] == True: # Spartan
  home = str(Path.home())
  dataset_file = home + "/" + config['dataset']
else: # local development machine
  data_folder = Path("../data/")
  dataset_file = data_folder / config['dataset']


# Read supported Twitter languages from file
twitter_language_file = config['supported_languages']
supported_languages = load_supported_languages(twitter_language_file)


if rank == 0:
  # Read dataset
  tweets = load_dataset(dataset_file)

  # dividing data into chunks
  chunks = [[] for _ in range(size)]
  for i, chunk in enumerate(tweets):
    chunks[i % size].append(chunk)

else:
  tweets = None
  chunks = None


# scatter requires a list of exactly comm.size elements as data to be scattered; so
tweets = comm.scatter(chunks, root=0)
print("Success")
data_processing(tweets)

#Wait until everyone is ready
comm.Barrier()


#print('Rank: ',rank, ', recvbuf received: ',recvbuf)

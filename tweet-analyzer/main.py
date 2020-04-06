#!/usr/bin/env python

import json
from pathlib import Path
from collections import Counter
from utilities import get_language, extract_hashtags, load_supported_languages, load_dataset, print_results
from mpi4py import MPI
import numpy as np


def data_processing(tweets):
  # Extract hashtags from all tweets and add to list
  extracted_hashtag = extract_hashtags(tweets)

  # Counting hashtags
  counter_hashtag = Counter(extracted_hashtag)

  # Couting tweet languages
  counter_language = Counter(tweet['doc']['metadata']['iso_language_code'] for tweet in tweets)

  results = {}
  results["hashtag"] = counter_hashtag
  results["language"] = counter_language
  return results


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

print("tweet-analyzer running on " + str(comm.size) + " cores")

# Read configuration options

path_current_dir = Path(__file__).parent.absolute()

path_config_file = path_current_dir.joinpath("config.json")

with open(str(path_config_file)) as config_file:
  config = json.load(config_file)

if config['production'] == True: # running on spartan
  home = str(Path.home())
  dataset_file = home + "/" + config['dataset']

else: # running on local development machine
  data_folder = Path("../data/")
  dataset_file = data_folder / config['dataset']


# Read supported Twitter languages from file
twitter_language_file = config['supported_languages']
path_language_file = path_current_dir.joinpath(twitter_language_file)
supported_languages = load_supported_languages(path_language_file)


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
results = data_processing(tweets)


print("\nResults from process " + str(rank), flush=True)
print_results(results["hashtag"], results["language"], supported_languages)


result_reduce = comm.gather(results, root=0)

#Wait until everyone is ready
comm.Barrier()


if rank == 0:
  counter_hashtag = Counter()
  counter_language = Counter()

  for counter_dict in result_reduce:
    counter_hashtag = counter_hashtag + counter_dict["hashtag"]
    counter_language = counter_language + counter_dict["language"]

  print("")
  print("Final results")
  print_results(counter_hashtag, counter_language, supported_languages)



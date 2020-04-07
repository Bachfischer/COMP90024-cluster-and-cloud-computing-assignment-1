#!/usr/bin/env python

import json
from pathlib import Path
from collections import Counter
from mpi4py import MPI
import os
from tweetanalyzer.utilities import load_supported_languages, print_results
from tweetanalyzer.data_processing import chunkify, process_wrapper

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

print("tweetanalyzer running on " + str(comm.size) + " cores")

# Read configuration options

path_current_dir = Path(__file__).parent.absolute()

path_config_file = path_current_dir.joinpath("config.json")

with open(str(path_config_file)) as config_file:
  config = json.load(config_file)

if config['production'] == True: # running on spartan - reading dataset relative to home directory
  home = str(Path.home())
  dataset_file = home + "/" + config['dataset']

else: # running on local development machine - reading dataset from data folder
  data_folder = Path("./data/")
  dataset_file = data_folder / config['dataset']


# Read supported Twitter languages from file
twitter_language_file = config['supported_languages']
path_language_file = path_current_dir.joinpath(twitter_language_file)
supported_languages = load_supported_languages(path_language_file)


if rank == 0:
  # Read dataset
  #tweets = load_dataset(dataset_file)
  dataset_size_total = os.path.getsize(dataset_file)
  dataset_size_per_process = dataset_size_total / size

  # dividing data into chunks
  instructions = []

  for chunkStart, chunkSize in chunkify(dataset_file, int(dataset_size_per_process)):
    instructions.append({"chunkStart": chunkStart, "chunkSize": chunkSize})



else:
  chunk_dimension = None
  instructions = None


# scatter requires a list of exactly comm.size elements as data to be scattered; so
chunk_dimension = comm.scatter(instructions, root=0)

print("chunkStart: " + str(chunk_dimension['chunkStart']) + " -  chunkSize " + str(chunk_dimension['chunkSize']))

results = process_wrapper(dataset_file, chunk_dimension["chunkStart"], chunk_dimension["chunkSize"])


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



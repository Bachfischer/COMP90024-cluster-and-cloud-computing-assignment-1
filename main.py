#!/usr/bin/env python

import json
from pathlib import Path
from collections import Counter
from mpi4py import MPI
import os
import argparse
from tweetanalyzer.utils import load_supported_languages, print_results
from tweetanalyzer.data_processing import DataProcessor

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

parser = argparse.ArgumentParser(description='Count # of hashtags and languages in Twitter dataset')
parser.add_argument('--dataset', type=str, default='bigTwitter.json', help='Path to Twitter dataset file')
parser.add_argument('--language-codes', type=str, default="language_codes.json", help='Path to list of supported language codes')
parser.add_argument('--batch-size', type=int, default=1024, help='Number of rows to be processed per batch')
args = parser.parse_args()

print("Tweetanalyzer running on " + str(comm.size) + " cores")

path_current_dir = Path(__file__).parent.absolute()

# Path to dataset in home directory
home_dir = str(Path.home())
path_dataset_file = home_dir + "/" + args.dataset

# Path to file with supported languages
path_language_file = str(path_current_dir.joinpath(args.language_codes))

# Read supported Twitter languages from file
supported_languages = load_supported_languages(path_language_file)

data_processor = DataProcessor()

if rank == 0:
  # Read dataset
  #tweets = load_dataset(dataset_file)
  dataset_size_total = os.path.getsize(path_dataset_file)
  dataset_size_per_process = dataset_size_total / size

  # dividing data into chunks
  chunks = []

  for chunkStart, chunkSize in data_processor.chunkify(path_dataset_file, int(dataset_size_per_process), dataset_size_total):
    chunks.append({"chunkStart": chunkStart, "chunkSize": chunkSize})

else:
  chunks = None

comm.Barrier()


# scatter requires a list of exactly comm.size elements as data to be scattered; so
chunk_per_process = comm.scatter(chunks, root=0)

print("chunkStart: " + str(chunk_per_process['chunkStart']) + " -  chunkSize " + str(chunk_per_process['chunkSize']))

# Start processing
data_processor.process_wrapper(path_dataset_file, chunk_per_process["chunkStart"], chunk_per_process["chunkSize"])

# Collect results
results = data_processor.retrieve_result()

print("\nResults from process " + str(rank), flush=True)
print_results(results["hashtag"], results["language"], supported_languages)


results_from_processes = comm.gather(results, root=0)

#Wait until everyone is ready
comm.Barrier()


if rank == 0:
  counter_hashtag = Counter()
  counter_language = Counter()

  for result in results_from_processes:
    counter_hashtag = counter_hashtag + result["hashtag"]
    counter_language = counter_language + result["language"]

  print("")
  print("Final results")
  print_results(counter_hashtag, counter_language, supported_languages)



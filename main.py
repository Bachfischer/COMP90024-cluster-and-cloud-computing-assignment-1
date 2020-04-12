#!/usr/bin/env python

import argparse
import os
from datetime import datetime
from pathlib import Path
from collections import Counter
from mpi4py import MPI
from tweetanalyzer.utils import load_supported_languages, \
    print_results_language_count, print_results_hashtag_count
from tweetanalyzer.data_processing import DataProcessor, chunkify

# Measure execution time from start of process to finish
START_TIME = datetime.now()
END_TIME = None

# Initialize MPI communication
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.Get_rank()

# Parse command line arguments
parser = argparse.ArgumentParser(
    description='Count # of hashtags and languages in Twitter dataset')
parser.add_argument('--dataset', type=str, default='bigTwitter.json',
                    help='Path to Twitter dataset file')
parser.add_argument('--language-codes', type=str, default="language_codes.json",
                    help='Path to list of supported language codes')
parser.add_argument('--batch-size', type=int, default=1024,
                    help='Number of rows to be processed per batch')
args = parser.parse_args()

print("Tweetanalyzer running on rank " + str(RANK) + " out of " + str(
    COMM.size) + " cores")

# Get current execution directory
path_current_dir = Path(__file__).parent.absolute()

# Create path to dataset in home directory
home_dir = str(Path.home())
path_dataset_file = home_dir + "/" + args.dataset

# Create path to file with supported languages (e.g. language_codes.json)
path_language_file = str(path_current_dir.joinpath(args.language_codes))

# Read supported Twitter languages from file
supported_languages = load_supported_languages(path_language_file)


def main():
    """Main method with implementation of scatter / gather logic for parallel
    execution in MPI environment
    """
    data_processor = DataProcessor(args.batch_size)

    if RANK == 0:

        # Get size of dataset and split dataset into equal parts
        dataset_size_total = os.path.getsize(path_dataset_file)
        dataset_size_per_process = dataset_size_total / SIZE

        # divide data into chunks and store byte positions of chunks in array
        chunks = []
        for chunkStart, chunkSize in chunkify(path_dataset_file,
                                              int(dataset_size_per_process),
                                              dataset_size_total):
            chunks.append({"chunkStart": chunkStart, "chunkSize": chunkSize})

    else:
        chunks = None

    # Wait until all processes have reached this step
    COMM.Barrier()

    # Scatter chunks to each individual process
    chunk_per_process = COMM.scatter(chunks, root=0)

    print("Rank " + str(RANK) + " received chunk - chunkStart: " + str(
        chunk_per_process['chunkStart']) + " -  chunkSize " +
          str(chunk_per_process['chunkSize']))

    # Start processing of chunk
    data_processor.process_wrapper(path_dataset_file,
                                   chunk_per_process["chunkStart"],
                                   chunk_per_process["chunkSize"])

    # Print results per process
    worker_results = data_processor.retrieve_results()

    # Print individual results for each process
    # print("\nResults from process " + str(RANK), flush=True)
    # print_results_hashtag_count(worker_results["hashtag"])
    # print_results_language_count(worker_results["language"], supported_languages)

    # Print execution time for worker nodes
    if RANK != 0:
        END_TIME = datetime.now()
        print("Execution time on core with rank " + str(RANK) + " was: " + str(
            END_TIME - START_TIME))

    # Gather results in master process (rank 0)
    worker_results = COMM.gather(worker_results, root=0)

    # Wait until everyone is ready
    COMM.Barrier()

    if RANK == 0:
        # Calculate final results using results from all worker processes
        counter_hashtag = Counter()
        counter_language = Counter()

        # Sum counter values for each worker process
        for result in worker_results:
            counter_hashtag = counter_hashtag + result["hashtag"]
            counter_language = counter_language + result["language"]

        # Print final results
        print("")
        print("Final results")
        print_results_hashtag_count(counter_hashtag)
        print_results_language_count(counter_language, supported_languages)
        END_TIME = datetime.now()
        print("Total execution time was: " + str(END_TIME - START_TIME))


if __name__ == "__main__":
    main()

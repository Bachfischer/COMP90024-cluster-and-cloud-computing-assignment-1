#!/usr/bin/env python

import json
from collections import Counter
from .utils import extract_hashtags, extract_language


def chunkify(path_to_dataset, chunk_size, total_size):
    with open(path_to_dataset, 'rb') as f:
        chunk_end = f.tell()

        while True:
            chunk_start = chunk_end
            # move from current position to current position + chunksize
            f.seek(f.tell() + chunk_size)
            f.readline()
            chunk_end = f.tell()
            if chunk_end > total_size:
                chunk_end = total_size
            yield chunk_start, chunk_end - chunk_start
            if chunk_end == total_size:
                break


def batchify(path_to_dataset, chunk_start, chunk_size, BATCH_SIZE):
    with open(path_to_dataset, 'rb') as f:
        batch_end = chunk_start

        while True:
            batch_start = batch_end
            # move from current position to current position + chunksize
            f.seek(batch_start + BATCH_SIZE)
            f.readline()
            batch_end = f.tell()
            if batch_end > chunk_start + chunk_size:
                batch_end = chunk_start + chunk_size
            yield batch_start, batch_end - batch_start
            # End of chunk
            if batch_end == chunk_start + chunk_size:
                break


class DataProcessor():

    def __init__(self, BATCH_SIZE):
        self.BATCH_SIZE = BATCH_SIZE
        self.extracted_hashtag_counter = Counter()
        self.extracted_language_counter = Counter()

    def retrieve_results(self):
        result = {"hashtag": self.extracted_hashtag_counter,
                  "language": self.extracted_language_counter}
        return result

    def process_tweet(self, tweet):
        # Extract hashtags from all tweets and add to list
        hashtag_list = extract_hashtags(tweet)
        for hashtag in hashtag_list:
            self.extracted_hashtag_counter[hashtag] += 1

        # Extract language
        language = extract_language(tweet)
        self.extracted_language_counter[language] += 1


    def process_wrapper(self, path_to_dataset, chunk_start, chunk_size):
        with open(path_to_dataset, 'rb') as f:
            batches = []

            # Split up chunk into batches of BATCH_SIZE
            for read_start, read_size in batchify(path_to_dataset, chunk_start,
                                                  chunk_size, self.BATCH_SIZE):
                batches.append({"batchStart": read_start, "batchSize": read_size})

            for batch in batches:
                f.seek(batch['batchStart'])

                if batch['batchSize'] > 0:
                    content = f.read(batch['batchSize']).splitlines()

                    for line in content:
                        line = line.decode('utf-8')  # Convert to utf-8
                        if line[-1] == ",":  # if line has comma
                            line = line[:-1]  # removing trailing comma
                        try:
                            tweet = json.loads(line)
                            self.process_tweet(tweet)

                        except Exception as e:
                            print("Error reading row from JSON file - ignoring")
                            print(line)
                else:
                    print("batchsize with size 0 detected")

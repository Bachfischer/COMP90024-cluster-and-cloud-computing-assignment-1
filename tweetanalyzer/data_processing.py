#!/usr/bin/env python

import json
from collections import Counter
from .utils import extract_hashtags, extract_language


def chunkify(path_to_dataset, chunk_size, total_size):
    """Divide dataset into smaller chunks by returning byte offset of chunk
    from beginning of file

    Keyword arguments:
    path_to_dataset -- Path to dataset to be split up
    chunk_size -- Size per chunk in bytes
    total_size -- Total size of dataset in bytes
    """
    with open(path_to_dataset, 'rb') as f:
        # Get current position in file
        chunk_end = f.tell()

        while True:
            chunk_start = chunk_end
            # Move from current position to current position + chunk_size
            f.seek(f.tell() + chunk_size)
            # Read until the next line to not break lines in the middle
            f.readline()
            # Get chunk_end as the offset from the beginning of the file in bytes
            chunk_end = f.tell()
            if chunk_end > total_size:
                chunk_end = total_size
            yield chunk_start, chunk_end - chunk_start
            if chunk_end == total_size:
                break


def batchify(path_to_dataset, chunk_start, chunk_size, BATCH_SIZE):
    """Divide chunk into smaller batches by returning byte offset of batch

    Keyword arguments:
    path_to_dataset -- Path to dataset to be split up
    chunk_start -- Byte offset of chunk from beginning of file
    chunk_size -- Size per chunk in bytes
    BATCH_SIZE -- Size of batch in bytes
    """
    with open(path_to_dataset, 'rb') as f:
        batch_end = chunk_start

        while True:
            batch_start = batch_end
            # move from current position of batch to current position + BATCH_SIZE
            f.seek(batch_start + BATCH_SIZE)
            # Read until the next line to not break lines in the middle
            f.readline()
            # Get batch_end as the offset from the beginning of the file in bytes
            batch_end = f.tell()
            if batch_end > chunk_start + chunk_size:
                batch_end = chunk_start + chunk_size
            yield batch_start, batch_end - batch_start
            # End of chunk
            if batch_end == chunk_start + chunk_size:
                break


class DataProcessor():

    def __init__(self, BATCH_SIZE):
        """Initialize data processing object with variables extracted_hashtag_
        counter and extracted_language_counter
        """
        self.BATCH_SIZE = BATCH_SIZE
        self.extracted_hashtag_counter = Counter()
        self.extracted_language_counter = Counter()

    def retrieve_results(self):
        """Return counting results in dictionary
        """
        result = {"hashtag": self.extracted_hashtag_counter,
                  "language": self.extracted_language_counter}
        return result

    def process_tweet(self, tweet):
        """Process tweet and perform counting operations

        Keyword arguments:
        tweet -- tweet in JSON format
        """
        # Extract hashtags and add to list
        hashtag_list = extract_hashtags(tweet)
        for hashtag in hashtag_list:
            self.extracted_hashtag_counter[hashtag] += 1

        # Extract language
        language = extract_language(tweet)
        self.extracted_language_counter[language] += 1


    def process_wrapper(self, path_to_dataset, chunk_start, chunk_size):
        """Main method executed by worker process to split chunk into smaller
        batches and process batches sequentially

        Keyword arguments:
        path_to_dataset -- Path to dataset to be split up
        chunk_start -- Byte offset of chunk from beginning of file
        chunk_size -- Size of chunk in bytes
        """
        with open(path_to_dataset, 'rb') as f:
            batches = []

            # Split up chunk into batches of size BATCH_SIZE
            for read_start, read_size in batchify(path_to_dataset, chunk_start,
                                                  chunk_size, self.BATCH_SIZE):
                batches.append({"batchStart": read_start, "batchSize": read_size})

            # Process batches sequentially
            for batch in batches:

                # Move to start position of batch
                f.seek(batch['batchStart'])

                if batch['batchSize'] > 0:
                    # Read in next batch in bytes as given per batchSize and
                    # split lines
                    content = f.read(batch['batchSize']).splitlines()

                    for line in content:
                        # Decode each line as utf-8 string
                        line = line.decode('utf-8')  # Convert to utf-8
                        if line[-1] == ",":  # if line has comma
                            line = line[:-1]  # removing trailing comma
                        try:
                            # Load tweet in JSON format
                            tweet = json.loads(line)
                            self.process_tweet(tweet)

                        except Exception as e:
                            print("Error reading row from JSON file - ignoring")
                            print(line)
                else:
                    print("batchsize with size 0 detected")

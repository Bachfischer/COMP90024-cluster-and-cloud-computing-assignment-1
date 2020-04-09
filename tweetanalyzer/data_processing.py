#!/usr/bin/env python

from collections import Counter
from .utils import extract_hashtags
import json


class DataProcessor(object):

    def __init__(self, BATCH_SIZE):
        self.BATCH_SIZE = BATCH_SIZE
        self.extracted_hashtags_counter = Counter()
        self.extracted_language_counter = Counter()


    # TODO refactor
    def retrieve_results(self):
        result = {}
        result["hashtag"] = self.extracted_hashtags_counter
        result["language"] = self.extracted_language_counter
        return result

    def process_tweet(self,tweet):
        # Extract hashtags from all tweets and add to list
        hashtag_list = extract_hashtags(tweet)
        for hashtag in hashtag_list:
            self.extracted_hashtags_counter[hashtag] += 1

        # Extract language
        language = tweet['doc']['metadata']['iso_language_code']
        self.extracted_language_counter[language] += 1


    # TODO: Error handling
    def process_wrapper(self, path_to_dataset, chunkStart, chunkSize):
        with open(path_to_dataset, 'rb') as f:

            batches = []

            # Split up chunk into batches of BATCH_SIZE
            for readStart, readSize in self.batchify(path_to_dataset, chunkStart, chunkSize):
                batches.append({"batchStart": readStart, "batchSize": readSize})


            for batch in batches:
                f.seek(batch['batchStart'])

                if batch['batchSize'] > 0:
                    content = f.read(batch['batchSize']).splitlines()

                    for line in content:
                        line = line.decode('utf-8')     # Convert to utf-8
                        if line[-1] == ",":    # if line has comma
                            line = line[:-1]  # removing trailing comma
                        try:

                            tweet = json.loads(line)
                            self.process_tweet(tweet)

                        except Exception as e:
                            print("Error reading row from JSON file")
                            print(line)

    def chunkify(self, path_to_dataset, chunkSize, totalSize):
        with open(path_to_dataset,'rb') as f:
            chunkEnd = f.tell()

            while True:
                chunkStart = chunkEnd
                # move from current position to current position + chunksize
                f.seek(f.tell() + chunkSize)
                f.readline()
                chunkEnd = f.tell()
                if chunkEnd > totalSize:
                    chunkEnd = totalSize
                yield chunkStart, chunkEnd - chunkStart
                if chunkEnd == totalSize:
                    break

    def batchify(self, path_to_dataset, chunkStart, totalSize):
        with open(path_to_dataset,'rb') as f:
            batchEnd = chunkStart

            while True:
                batchStart = batchEnd
                # move from current position to current position + chunksize
                f.seek(batchStart + self.batchSize)
                f.readline()
                batchEnd = f.tell()
                if batchEnd > chunkStart + totalSize:
                    batchEnd = chunkStart + totalSize
                yield batchStart, batchEnd - batchStart
                # End of chunk
                if batchEnd == chunkStart + totalSize:
                    break
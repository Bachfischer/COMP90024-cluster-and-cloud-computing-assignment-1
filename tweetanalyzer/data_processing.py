import os
from collections import Counter
from .utilities import extract_hashtags
import json

class DataProcessor(object):

    def __init__(self):
        self.extracted_hashtags_counter = Counter()

        self.extracted_language_counter = Counter()


    # TODO refactor
    def perform_analysis(self):

        result = {}
        result["hashtag"] = self.extracted_hashtags_counter

        result["language"] = self.extracted_language_counter

        return result

    def process_tweet(self,tweet):
        # Extract hashtags from all tweets and add to list
        hashtags = extract_hashtags(tweet)
        row_hashtag_counter = Counter(hashtags)
        self.extracted_hashtags_counter = self.extracted_hashtags_counter + row_hashtag_counter

        # Extract language
        language = tweet['doc']['metadata']['iso_language_code']
        self.extracted_language_counter[language] += 1


    # TODO: Error handling
    def process_wrapper(self, path_to_dataset, chunkStart, chunkSize):
        with open(path_to_dataset, 'rb') as f:

            batchSize = 1024*1024

            readBatches = []

            for readStart, readSize in self.batchify(path_to_dataset, chunkStart, batchSize, chunkSize):
                readBatches.append({"batchStart": readStart, "batchSize": readSize})


            for batch in readBatches:
                f.seek(batch['batchStart'])
                #print(str(batch['batchSize']))

                if batch['batchSize'] > 0:
                    content = f.read(batch['batchSize']).splitlines()

                    for line in content:
                        line = line[:-1]  # removing trailing comma
                        try:
                            # TODO: Find more elegant solution for parsing corrupt JSON
                            tweet = json.loads(line)
                            self.process_tweet(tweet)
                            #data = json.loads(content + "]}")  # adding missing brackets - expecting to add "]}"

                        except Exception as e:
                            print("Error reading row from JSON file")
                            #print(line)

    # TODO: Replace size with parameter as per Liams comment
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
                if chunkEnd >= totalSize:
                    break

    def batchify(self, path_to_dataset, chunkStart, batchSize, totalSize):
        with open(path_to_dataset,'rb') as f:
            batchEnd = chunkStart

            while True:
                batchStart = batchEnd
                # move from current position to current position + chunksize
                f.seek(batchStart + batchSize)
                f.readline()
                batchEnd = f.tell()
                if batchEnd > chunkStart + totalSize:
                    batchEnd = chunkStart + totalSize
                yield batchStart, batchEnd - batchStart
                # End of chunk
                if batchEnd == chunkStart + totalSize:
                    break
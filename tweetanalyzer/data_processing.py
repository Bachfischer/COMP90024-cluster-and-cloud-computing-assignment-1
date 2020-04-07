import os
from collections import Counter
from .utilities import extract_hashtags
import json

def process_tweets(tweets):
  # Extract hashtags from all tweets and add to list
  extracted_hashtag = extract_hashtags(tweets)

  # Counting hashtags
  counter_hashtag = Counter(extracted_hashtag)

  # Couting tweet languages
  counter_language = Counter(tweet['doc']['metadata']['iso_language_code'] for tweet in tweets)

  result = {}
  result["hashtag"] = counter_hashtag
  result["language"] = counter_language
  return result


# TODO: Error handling
def process_wrapper(path_to_dataset, chunkStart, chunkSize):
    with open(path_to_dataset) as f:
        f.seek(chunkStart)
        content = f.read(chunkSize).splitlines()
        data = []
        while True:
            try:
                # TODO: Fine more elegant solution for parsing corrupt JSON
                print("Trying to load JSON file")
                for line in content:
                    line = line[:-1] # removing trailing comma
                    data.append(json.loads(line))
                #data = json.loads(content + "]}")  # adding missing brackets - expecting to add "]}"

            except Exception as e:
                print("Error loading JSON file - trying to fix corrupt data")
                content = content[:-1]  # Removing last character - expecting to remove ","
                continue
            break

        tweets = data
        result = process_tweets(tweets)
        return result

# TODO: Replace size with parameter as per Liams comment
def chunkify(path_to_dataset,dataset_size_per_process):
    total_file_size = os.path.getsize(path_to_dataset)
    with open(path_to_dataset,'r') as f:
        chunkEnd = f.tell()

        while True:
            chunkStart = chunkEnd
            # move from current position to current position + chunksize
            f.seek( f.tell() + dataset_size_per_process)
            f.readline()
            chunkEnd = f.tell()
            yield chunkStart, chunkEnd - chunkStart
            if chunkEnd >= total_file_size:
                break
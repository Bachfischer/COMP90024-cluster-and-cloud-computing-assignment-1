# Cluster and Cloud Computing - Assignment 1

## TODO

-Get latest list of supported languages from Twitter, especially for code "und" (Matthias)

-Setup Code Linter

- Identify the languages used for tweeting and the number of times the language is used for the provided tweets
https://developer.twitter.com/en/docs/twitter-for-websites/twitter-for-websites-supported-languages/overview

- Identify the top 10 most commonly used hashtags and the number of times they appear. A matching hashtag string can match if it has upper/lower case exact substrings, e.g. #covid19 and #COVID19 are a match. A hashtag should follow the Twitter rules, e.g. no spaces and no punctuation are allowed in a hashtag


## Notes

"The master process should not load all the data first - this will result in a bottleneck. If the master tried to read in ALL of the data then it will crash as it can’t "

"Make sure to use a wall size that is neither too long or too short. If to long, it will never get scheduled, and if too short, it will be canceled before finishing.
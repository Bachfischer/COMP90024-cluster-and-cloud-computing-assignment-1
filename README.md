# Cluster and Cloud Computing - Assignment 1

## Overview

This repository contains the source code for assignment 1 of the COMP90024 Cluster and Cloud Computing course at the University of Melbourne.

**Submission Details:**
- Student name: Matthias Bachfischer

- Student ID: 1133751

## Project structure

* `data/` -- datasets used for testing 
* `doc/` -- documentation and implementation notes
* `output/` -- Output from previous submission runs on Spartan
* `playground/` -- scripts used for Twitter API communication
* `slurm/` -- slurm scripts for submission to Spartan queue
* `tweetanalyzer/` -- helper and utility functions

## Instructions

To submit a job to the Spartan cluster, run the command ```sbatch path_to_slurm_script``` and replace ```path_to_slurm_script``` with the name of the SLURM script that you want to run.

## Notes

### Task 1: Identification of common hashtags

Identify the top 10 most commonly used hashtags and the number of times they appear. A matching hashtag string can match if it has upper/lower case exact substrings, e.g. #covid19 and #COVID19 are a match. A hashtag should follow the Twitter rules, e.g. no spaces and no punctuation are allowed in a hashtag - any string following a # up until a space or punctuation character is a valid hashtag string (except underscore _).


### Part 2: Language counting

Identify the languages used for tweeting and the number of times the language is used for the provided tweets

Documentation: https://developer.twitter.com/en/docs/twitter-for-websites/twitter-for-websites-supported-languages/overview

Standard for language code: https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes


## SPARTAN partitions

**Cloud**

This partition is best suited for general-purpose single-node jobs. Multiple node jobs will work, but communication between nodes will be comparatively slow.

**Physical**

Each node is connected by high-speed 25Gb networking with 1.15 µsec latency, making this partition suited to multi-node jobs (e.g. those using OpenMPI).
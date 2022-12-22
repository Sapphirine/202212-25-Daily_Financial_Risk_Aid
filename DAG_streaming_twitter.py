"""
DAG_streaming_twitter.py

Fernando Rodriguez-Guzman Jr
fr2510 | fr2510@columbia.edu
December 23rd, 2022
EECS E6893: Big Data Analytics
Final Project: Daily Financial Investment Risk Aid 

This DAG is used to stream data from twitter API.

"""
from datetime import datetime, timedelta
from textwrap import dedent
import logging
import time

from pyspark.streaming import StreamingContext
from pyspark import SparkConf,SparkContext
from pyspark.sql import Row,SQLContext
import pandas as pd
import subprocess
import requests
import pprint
import shutil
import sys
import os

# The DAG object to instantiate a DAG
from airflow import DAG

# Airflow Operators
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

####################################################
# DEFINE GLOBAL VARIABLES
####################################################

# Storage Directory
output_directory_hashtags = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/tweets/hashtags'
output_directory_wordcount = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/tweets/wordcount'
combined_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/tweets/combined'

# output table and columns name
columns_name_hashtags = ['hashtags', 'count']
columns_name_wordcount = ['time', 'count', 'word']

# parameter
IP = 'localhost'    # ip port
PORT = 9001       # port

STREAMTIME = 60          # time that the streaming process runs

# Words you should filter and do word count
WORD = ['VSMPX', 'VFIAX', 'AGTHX', 'OXY', 'XOM', 'VRTX', 'ENPH', 'FOUR', 'ON']


####################################################
# DEFINE PYTHON FUNCTIONS
####################################################

def print_start():
    # Print to screen
    print(f'Starting DAG Process')


# Save to file storage
def saveToStorage(rdd, output_directory, columns_name, mode):
    """
    Save each RDD in this DStream to google storage
    Args:
        rdd: input rdd
        output_directory: output directory in google storage
        columns_name: columns name of dataframe
        mode: mode = "overwrite", overwrite the file
              mode = "append", append data to the end of file
    """
    if not rdd.isEmpty():
        (rdd.toDF( columns_name ).write.save(output_directory, format="csv", mode=mode))

def unify_csv(directory, columns_name, combined_file_name, final_dest):

    #read the path
    file_path = directory

    # Create a list with all the files from the directory
    file_list = os.listdir(file_path)

    # Make a list of all non-csv files
    items_to_remove = []
    for file in file_list:
        if file[0] == ".":
            items_to_remove.append(file)
        
        elif file[0] == "_":
            items_to_remove.append(file)
        
        else:
            pass

    # Remove non-CSV files from list
    for i in range(len(items_to_remove)):
        file_list.remove(items_to_remove[i])

    # Add Path to name
    for i in range(len(file_list)):
        file_list[i] = (f'{directory}/{file_list[i]}')

    # Remove Empty CSVs
    # Make empty list
    empty_items_to_remove = []
    for file in file_list:
        if os.stat(file).st_size == 0:
            empty_items_to_remove.append(file)
            print(f'File is empty: {file}')
        else:
            print(f'File is NOT empty: {file}')

    # Remove empty CSVs
    for i in range(len(empty_items_to_remove)):
        file_list.remove(empty_items_to_remove[i])

    # Make a Columns List Dictionary
    columns_dict = {}
    for j in range(len(columns_name)):
            columns_dict[j] = columns_name[j]
    
    # Add Column Names
    for i in range(len(file_list)):
        name_df = pd.read_csv(file_list[i], header=None)        
        name_df.rename(columns=columns_dict, inplace=True)
        name_df.to_csv(file_list[i], index=False) # save to new csv file

    # Combine files into one CSV
    # merging two csv files
    df = pd.concat(
        map(pd.read_csv, file_list), ignore_index=True)
    
    df.to_csv(f'{directory}/{combined_file_name}')

    # Move the File
    os.rename(f'{directory}/{combined_file_name}', f'{final_dest}/{combined_file_name}')

    # Cleanup Working Folder (aka delete it)
    shutil.rmtree(directory)

def hashtagCount(words):
    # Filters words by '#' only
    hashtag_words_filter = words.filter(lambda x: x.startswith('#'))
    
    # Form key value pair then Reduce the hashtags by aggregeation
    hashtag_pairs = hashtag_words_filter.map(lambda word: (word, 1))
    hashtagCounts = hashtag_pairs.reduceByKey(lambda x, y: x + y)
    
    # Sum the count from this and previous stream
    def updateFunction(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount) 
    
    hashtagCounts = hashtag_pairs.updateStateByKey(updateFunction)
    
    # Sort hashtags in decending order
    hashtagCounts_sorted = hashtagCounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    
    return hashtagCounts_sorted

def wordCount(words):
    # Filter the hashtags that contain the words in the list WORD
    words_filtered = words.filter(lambda x: x in WORD)
    
    # Window word count
    word_pairs = words_filtered.map(lambda word: (word, 1))
    wordCount_pairs = word_pairs.reduceByKey(lambda x, y: x + y)
    windoWedwordCount = wordCount_pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 10, 10)    # Count every 10 seconds
    
    # Add a datetime type
    date_and_windoWedwordCount = windoWedwordCount.map(lambda word: (datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z%z"), word[1], word[0]))
    
    return date_and_windoWedwordCount

def start_stream():
    # Spark settings
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName("TwitterStreamApp")

    # create spark context with the above configuration
    sc = SparkContext.getOrCreate(conf=conf)   # added getOrCreate
    sc.setLogLevel("ERROR")

    # create sql context, used for saving rdd
    sql_context = SQLContext(sc)

    # create the Streaming Context from the above spark context with batch interval size 5 seconds
    ssc = StreamingContext(sc, 5)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("~/checkpoint_TwitterApp")

    # read data from port 9001
    dataStream = ssc.socketTextStream(IP, PORT)
    dataStream.pprint()

    words = dataStream.flatMap(lambda line: line.split(" "))

    # calculate the accumulated hashtags count sum from the beginning of the stream
    topTags = hashtagCount(words)
    topTags.pprint()

    # Calculte the word count during each time period 6s
    wordCounts = wordCount(words)
    wordCounts.pprint()

    # Save to cloud storage
    topTags.foreachRDD(lambda rdd: saveToStorage(rdd, output_directory_hashtags, columns_name_hashtags, mode="overwrite"))  # OG
    wordCounts.foreachRDD(lambda rdd: saveToStorage(rdd, output_directory_wordcount, columns_name_wordcount, mode="append"))  # OG

    # start streaming process, wait and then stop.
    ssc.start()

    time.sleep(STREAMTIME)

    ssc.stop(stopSparkContext=False, stopGraceFully=True)

    # put the temp results into one csv
    unify_csv(output_directory_hashtags, columns_name_hashtags, 'combined_hashtags.csv', combined_path)
    unify_csv(output_directory_wordcount, columns_name_wordcount, 'combined_wordcount.csv', combined_path)

def print_end():
    # Print to screen
    print(f'End of DAG Process')

############################################
# DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################

default_args = {
    'owner': 'fr2510',
    'depends_on_past': False,
    'email': ['fr2510@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    '3_Final_Project_Streaming_Twitter_Daily_Financial_Investment_Risk_Aid',
    default_args=default_args,
    description='DAG for Final Project\'s Streaming Twitter Data ',
    schedule_interval='*/05 * * * *',   # cron: min, hr, day, mth, day of week
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['Final_Project_Twitter_Client'],
) as dag:

##########################################
# DEFINE AIRFLOW OPERATORS
##########################################

    t0 = PythonOperator(
        task_id='t0',
        python_callable=print_start,
    )

    t1 = PythonOperator(
        task_id='t1',
        python_callable=start_stream,
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=print_end,
    )

##########################################
# DEFINE TASKS HIERARCHY
##########################################

    t0 >> t1    # Print start to terinal -> Start Stream
    t1 >> t2    # Stream -> Reach end and print ending state to terminal





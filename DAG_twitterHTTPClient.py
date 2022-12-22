"""
DAG_twitterHTTPClient.py

Fernando Rodriguez-Guzman Jr
fr2510 | fr2510@columbia.edu
December 23rd, 2022
EECS E6893: Big Data Analytics
Final Project: Daily Financial Investment Risk Aid 

This DAG is used to pull data from twitter API and send data to
Spark Streaming process using socket. It acts like a client of
twitter API and a server of spark streaming. It open a listening TCP
server socket, and listen to any connection from TCP client. After
a connection established, it send streaming data to it. 

"""
from datetime import datetime, timedelta
from textwrap import dedent
import logging
import time

from tweepy import OAuthHandler
from tweepy import Stream
import socket
import tweepy
import re

# The DAG object to instantiate a DAG
from airflow import DAG

# Airflow Operators
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

####################################################
# DEFINE GLOBAL VARIABLES
####################################################

BEARER_TOKEN = 'XXXXXXXX'   # Removed my token for security reasons as this code will be open source

# the tags to track
tags = ['#','VSMPX', 'VFIAX', 'AGTHX', 'OXY', 'XOM', 'VRTX', 'ENPH', 'FOUR', 'ON']

####################################################
# DEFINE PYTHON FUNCTIONS
####################################################

def print_start():
    # Print to screen
    print(f'Starting DAG Process')

class MyStream(tweepy.StreamingClient):

    global client_socket
    def on_tweet(self, tweet):
        try:
            msg = tweet
            print('TEXT:{}\n'.format(msg.text))
            client_socket.send( msg.text.encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            self.disconnect()
            return False
        
    def on_error(self, status):
        print(status)
        return False


def sendData(c_socket, tags):
    """
    send data to socket
    """
    global client_socket
    client_socket = c_socket
    stream = MyStream(BEARER_TOKEN)

    for tag in tags:
        stream.add_rules(tweepy.StreamRule(tag))

    stream.filter()

class twitter_client:
    def __init__(self, TCP_IP, TCP_PORT):
      self.s = s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.s.bind((TCP_IP, TCP_PORT))

    def run_client(self, tags):
      try:
        self.s.listen(1)
        while True:
          print("Waiting for TCP connection...")
          conn, addr = self.s.accept()
          print("Connected... Starting getting tweets.")
          sendData(conn,tags)
          conn.close()
      except KeyboardInterrupt:
        exit

def start_client():
    client = twitter_client("localhost", 9001)
    client.run_client(tags)

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
    '2_Final_Project_TwitterClient_Daily_Financial_Investment_Risk_Aid',
    default_args=default_args,
    description='DAG for Final Project\'s Twitter Client ',
    schedule_interval='0 12 * * *',   # At 12:00 UTC aka 07:00 EST everyday 
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
        python_callable=start_client,
    )

##########################################
# DEFINE TASKS HIERARCHY
##########################################

    t0 >> t1

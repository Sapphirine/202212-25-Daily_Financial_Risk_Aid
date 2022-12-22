"""
DAG_Final_Project_Stock_Get.py

Fernando Rodriguez-Guzman Jr
fr2510 | fr2510@columbia.edu
December 23rd, 2022
EECS E6893: Big Data Analytics
Final Project: Daily Financial Investment Risk Aid 

Selection To Track
    Mutual Funds:
            VSMPX   Vanguard Total Stock Market Index Fund;Institutional Plus
            VFIAX   Vanguard 500 Index Fund;Admiral
            AGTHX   American Funds Growth Fund of America;A

    Stocks:
            OXY     Occidental Petroleum Corp.  
            XOM     Exxon Mobil Corp.
            VRTX    Vertex Pharmaceuticals Inc.

    Fast Growing Stocks:
            ENPH    Enphase Energy Inc.
            FOUR    Shift4 Payments Inc.
            ON      ON Semiconductor Corp.

"""

from datetime import datetime, timedelta
from textwrap import dedent
import logging
import time


from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import yfinance as yf
import pandas_ta as ta
import pandas as pd
import csv
import os

# The DAG object to instantiate a DAG
from airflow import DAG

# Airflow Operators
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

####################################################
# DEFINE GLOBAL VARIABLES
####################################################
# Stocks to track
ticker = ['VSMPX', 'VFIAX', 'AGTHX', 'OXY', 'XOM', 'VRTX', 'ENPH', 'FOUR', 'ON']

vsmpx = yf.Ticker(ticker[0])    # Vanguard Total Stock Market Index Fund;Institutional Plus
vfiax = yf.Ticker(ticker[1])    # Vanguard 500 Index Fund;Admiral
agthx = yf.Ticker(ticker[2])    # American Funds Growth Fund of America;A
oxy = yf.Ticker(ticker[3])      # Occidental Petroleum Corp.
xom = yf.Ticker(ticker[4])      # Exxon Mobil Corp.
vrtx = yf.Ticker(ticker[5])     # Vertex Pharmaceuticals Inc.
enph = yf.Ticker(ticker[6])     # Enphase Energy Inc.
four = yf.Ticker(ticker[7])     # Shift4 Payments Inc.
on = yf.Ticker(ticker[8])       # ON Semiconductor Corp.


# CSV Path
vsmpx_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/vsmpx_stock_history.csv'
vfiax_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/vfiax_stock_history.csv'
agthx_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/agthx_stock_history.csv'
oxy_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/oxy_stock_history.csv'
xom_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/xom_stock_history.csv'
vrtx_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/vrtx_stock_history.csv'
enph_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/enph_stock_history.csv'
four_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/four_stock_history.csv'
on_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/on_stock_history.csv'

mae_csv_path = '/home/fr2510/EECS6893_Daily_Financial_Aid/Financial_Data/stock_mae_history.csv'

#Dictionary containig history of stocks
hist = {}

# Timestamp String
str_date_time = datetime.now().strftime("%Y-%m-%d")

####################################################
# DEFINE PYTHON FUNCTIONS
####################################################

count = 0

def print_start():
    # Print to screen
    print(f'Starting DAG Process')

def sleep_function():
    """sleep delay"""
    time.sleep(1)

def get_stock(stock_string, stock_ticker, stock_csv_path ):
    """
    This function fetches the stock 
    information and saves it as a CSV.
    """
    
    # Append stock informaiton
    global hist
    hist[stock_string] = stock_ticker.history(period='max')

    # Save CSVs
    hist[stock_string].to_csv(stock_csv_path)

    """Process the data for training"""
    # Remove other columns, but keep 'Close' price
    stock_df = hist[stock_string][['Close']]

    # Add an Exponential Moving Average of the latest 10 stocks
    stock_df.ta.ema(close='Close', length=10, append=True)   # Add EMA to dataframe by appending
    stock_df = stock_df.iloc[10:] # Delete first ten rows since we lsot them due to averaging

    # Split the data into training and test data with 80% to 20% ratio
    stock_X_train, stock_X_test, stock_y_train, stock_y_test = train_test_split(stock_df[['Close']], stock_df[['EMA_10']], test_size=.2)

    """Training My Model"""
    stock_model = LinearRegression() # Create Regression Model
    stock_model.fit(stock_X_train, stock_y_train)  # Train the model
    stock_y_pred = stock_model.predict(stock_X_test)   # Use model to make predictions and calculate MAE
    stock_future_pred = stock_model.predict(stock_df[['Close']])   # Use model to make future predictions

    # Printout Metrics
    logging.info(f"Mean Absolute Error for {stock_string}: {mean_absolute_error(stock_y_test, stock_y_pred)}")    # log MAE

    """Save MAE Entry to CSV"""
    if os.path.exists(mae_csv_path):
        logging.info(f"File Exists: {os.path.exists(mae_csv_path)}")    # log that file exists
        
        # Append to CSV MAE Data
        data = {'Date': [str_date_time], 'Stock': [stock_string], 'Close': [stock_df['Close'][-1]], 'Predicted_Close': stock_future_pred[-1], 'Mean_Abs_Error': [mean_absolute_error(stock_y_test, stock_y_pred)]}
         
        # Make data frame of above data
        df = pd.DataFrame(data)
         
        # Append data frame to CSV file
        df.to_csv(mae_csv_path, mode='a', index=False, header=False)
         
        # print message
        logging.info('Data appended successfully')    # log append action

        
    else:
        logging.info(f"File Does Not Exist: {os.path.exists(mae_csv_path)}")    # log that file does not exist
        logging.info(f"Creating CSV as {mae_csv_path}")    # log creating CSV
        
        # Create CSV
        headers = ['Date', 'Stock', 'Close', 'Predicted_Close', 'Mean_Abs_Error']
        with open(mae_csv_path, 'w', newline='') as csvfile:
           csvwriter = csv.writer(csvfile)
           csvwriter.writerow(headers)
           csvfile.close()

        """Save MAE Entry to CSV"""    
        #Append to CSV MAE Data
        data = {'Date': [str_date_time], 'Stock': [stock_string], 'Close': [stock_df['Close'][-1]], 'Predicted_Close': stock_future_pred[-1], 'Mean_Abs_Error': [mean_absolute_error(stock_y_test, stock_y_pred)]}
         
        # Make data frame of above data
        df = pd.DataFrame(data)
         
        # append data frame to CSV file
        df.to_csv(mae_csv_path, mode='a', index=False, header=False)
         
        # print message
        logging.info('Data appended successfully')    # log append action

def vsmpx_stock():
    get_stock('vsmpx', vsmpx, vsmpx_csv_path)

def vfiax_stock():
    get_stock('vfiax', vfiax, vfiax_csv_path)

def agthx_stock():
    get_stock('agthx', agthx, agthx_csv_path)

def oxy_stock():
    get_stock('oxy', oxy, oxy_csv_path)

def xom_stock():
    get_stock('xom', xom, xom_csv_path)

def vrtx_stock():
    get_stock('vrtx', vrtx, vrtx_csv_path)

def enph_stock():
    get_stock('enph', enph, enph_csv_path)

def four_stock():
    get_stock('four', four, four_csv_path)

def on_stock():
    get_stock('on', on, on_csv_path)

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
    '1_Final_Project_Daily_Financial_Investment_Risk_Aid',
    default_args=default_args,
    description='DAG for Final Project: Daily Financial Investment Risk Aid ',
    schedule_interval='0 12 * * *',   # At 12:00 UTC aka 07:00 EST everyday 
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['Final_Project'],
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
        python_callable=vsmpx_stock,
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=vfiax_stock,
    )

    t3 = PythonOperator(
        task_id='t3',
        python_callable=agthx_stock,
    )

    t4 = PythonOperator(
        task_id='t4',
        python_callable=oxy_stock,
    )

    t5 = PythonOperator(
        task_id='t5',
        python_callable=xom_stock,
    )

    t6 = PythonOperator(
        task_id='t6',
        python_callable=vrtx_stock,
    )

    t7 = PythonOperator(
        task_id='t7',
        python_callable=enph_stock,
    )

    t8 = PythonOperator(
        task_id='t8',
        python_callable=four_stock,
    )

    t9 = PythonOperator(
        task_id='t9',
        python_callable=on_stock,
    )

    t10 = PythonOperator(
        task_id='t10',
        python_callable=sleep_function,
    )

    t11 = PythonOperator(
        task_id='t11',
        python_callable=print_end,
    )

##########################################
# DEFINE TASKS HIERARCHY
##########################################

    # task dependencies
    t0 >> [t1, t2, t3, t4, t5, t6, t7, t8, t9]  # Fetch stock data
    [t1, t2, t3, t4, t5, t6, t7, t8, t9] >> t10  # Wait for all to be done
    t10 >> t11    # Done

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

# CONSTANTS
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate = 'exchange_rate.csv'
output_csv = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage 
    of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, 'a') as file:
        file.write(timestamp + ' => ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df = pd.DataFrame(columns=table_attribs)
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                'Name': col[1].find_all('a')[1].contents[0],
                'MC_USD_Billion': float(col[2].contents[0][:-1])
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = pd.read_csv(csv_path)
    USD_list = df['MC_USD_Billion'].to_list()
    EUR_list = [np.round(float(x) * exchange_df['Rate'].iloc[0], 2) for x in USD_list]
    GBP_list = [np.round(float(x) * exchange_df['Rate'].iloc[1], 2) for x in USD_list]
    INR_list = [np.round(float(x) * exchange_df['Rate'].iloc[2], 2) for x in USD_list]
    df['MC_GBP_Billion'] = GBP_list
    df['MC_EUR_Billion'] = EUR_list
    df['MC_INR_Billion'] = INR_list

    print(f"value for MC_EUR_Billion: {df['MC_EUR_Billion'][4]}")
    return df


# def transform(df, csv_path):
#     dict = dataframe.set_index('Col_1_header').to_dict()['Col_2_header']
#     exchange_df = pd.read_csv(csv_path)
#     exchange_dict = exchange_df.setindex('Currency).to_dict()['Rate']
#     df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_df[exchange_df['Currency'] == 'GBP']['Rate'], 2)
#     df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_df[exchange_df['Currency'] == 'EUR']['Rate'], 2)
#     df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_df[exchange_df['Currency'] == 'INR']['Rate'], 2)
#     print(df['MC_EUR_Billion'][4])
#     return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, ['Name', 'MC_USD_Billion'])
log_progress('Data extraction complete. Initiating Transformation process')
df =  transform(df, exchange_rate)
log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df, output_csv)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')
query_statement = f'SELECT * FROM Largest_banks'
run_query(query_statement, sql_connection)
query_statement = f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement, sql_connection)
query_statement = f'SELECT Name from Largest_banks LIMIT 5'
run_query(query_statement, sql_connection)
log_progress('Process Complete')
sql_connection.close()
log_progress('Server Connection closed')

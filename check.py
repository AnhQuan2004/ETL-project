# Code for ETL operations on Country-GDP data
import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import sqlite3
from datetime import datetime

url = "https://www.boxofficemojo.com/year/2023/"
table_attribs = ["Name", "Gross", "Release Date"]
table_name = "Movies_Rating"
db_name = "Movies.db"
csv_path = "Movies.csv"

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('table')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            name = col[1].find('a')
            name_text = name.text.strip() if name else ''
            data_dict = {"Name": name_text,
                         "Gross": col[7].text[1:],
                         "Release Date": col[8].text}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df
def transform(df):

    film_list = df['Gross'].tolist()

    # Remove commas 
    film_list = [x.replace(',','') for x in film_list]

    # Now convert to floats
    film_list = [float(x) for x in film_list]

    film_list = [np.round(x/1000000,2) for x in film_list]

    df['Gross'] = film_list

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with open("log.txt", "a") as log_file:
        log_file.write(timestamp + " - " + message + "\n")
    


''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("ETL process started")
df = extract(url, table_attribs)

log_progress("Data extracted from the website")
df = transform(df)

log_progress("Data transform completed. Loading to CSV")
load_to_csv(df, csv_path)

log_progress("Loading to CSV completed. Loading to DB")
sql_connection = sqlite3.connect("film_Rating.db")

log_progress("Loading to DB completed. Running query")
load_to_db(df, sql_connection, table_name)

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)

log_progress("ETL process completed")
sql_connection.close()

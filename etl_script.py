import os
import sys
import petl
import petl.io.xlsx
# import pymssql
import pyodbc
import configparser
import requests
import datetime
import json
import decimal

# Get data from configuration file
config = configparser.ConfigParser()

try:
    config.read('etl_script.ini')
except Exception as e:
    print(f"Cannot read the configuration file: {str(e)}")
    sys.exit()


# Read settings for configuration data
startdate = config['CONFIG']['startdate']
url = config['CONFIG']['url']
server = config['CONFIG']['server']
database = config['CONFIG']['database']

# Request data from URL
try:
    BankOfCanada_response = requests.get(url + startdate)
except Exception as e:
    print(f"Could not make request: {str(e)}")
    sys.exit()

# print(BankOfCanada_response.text)

# Initialize list for storing date and rate
BankOfCanada_Dates = []
BankOfCanada_Rates = []

# Check response status
if BankOfCanada_response.status_code == 200:
    BankOfCanada_raw = json.loads(BankOfCanada_response.text)

    # Append data to our list
    for row in BankOfCanada_raw['observations']:
        BankOfCanada_Dates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BankOfCanada_Rates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # Create petl table froum the array
    exchangeRates = petl.fromcolumns([BankOfCanada_Dates,BankOfCanada_Rates],header=['date','rate'])

    # Load Expenses.xlsx document
    try:
        expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx',sheet='Github')
    except Exception as e:
        print("Cannot load the Expenses.xlsx document")
        sys.exit()
    
    # Join the exchangerates table with expenses.xlsx with date as key
    expenses = petl.outerjoin(exchangeRates,expenses,key='date')

    # Impute the missing values
    expenses = petl.filldown(expenses,'rate')

    # Remove date with no expenses
    expenses = petl.select(expenses, lambda rec: rec.USD != None)

    # Add Canadian Dollars Column (CAD)
    expenses = petl.addfield(expenses, 'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    # print(expenses)

    # Initialize database connection
    try:
        conn_string = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=LAPTOP-E9N1N3UU\\MSSQLSERVER01;"
            "Database=ETLDemo;"
            "Trusted_Connection=yes;"
        )
        connection = pyodbc.connect(conn_string)
        print("Connected to database successfully!")

    except Exception as e:
        print(f"Cannot connect to database: {str(e)}")
        sys.exit()
    
    # Populate the expenses into database table
    try:
        petl.io.todb(expenses, connection, 'Expenses')
    except Exception as e:
        print(f"Cannot write data to database {str(e)}")
        sys.exit()
    
    connection.close()
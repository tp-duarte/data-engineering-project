# script for processing and writing data to a SQL Database 

# packages required

import chardet as ct
import csv
import os 
import pandas as pd 
from pandas.errors import ParserError
import psycopg2


# constants 

TABLE = 'projeto' 
DATABASE = 'inovacao'
DATAPATH = 'D:\opensense\projeto\data\BD - Pesquisa e Inovação - Reitoria e campus 2022.csv'


# reading data and getting encoding type

with open(DATAPATH, 'rb') as f:
    result = ct.detect(f.read())

encoded_value = list(result.values())[0]


# processing datafile

with open(DATAPATH, 'r', encoding = encoded_value) as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader) # Read header row
    num_fields = len(header) # Count number of fields in header
    prev_row = header
    with open('processed_data.csv', 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header) # Write header row to output file
        for row in reader:
            try:
                if len(row) != len(prev_row):
                    raise ParserError('Unexpected number of fields')
                # Process row normally
                processed_row = []
                for i in range(num_fields):
                    field = row[i]
                    # Processing row to the csv file
                    processed_row.append(field)
                writer.writerow(processed_row)
                prev_row = row
            except ParserError:
                # Handle row with unexpected number of fields
                missing_fields = len(prev_row) - len(row)
                row += ['' for i in range(missing_fields)]
                # Continue processing row
                processed_row = []
                for i in range(num_fields):
                    field = row[i]
                    # Processing row to the csv file
                    processed_row.append(field)
                writer.writerow(processed_row)
                prev_row = row
            except Exception as e:
                # Handle other exceptions
                print(e)
                continue

# processing and removing file generated from system 

data = pd.read_csv('processed_data.csv')
os.remove('processed_data.csv')

# transforming data

data['Valor total em bolsas para o projeto'] = pd.to_numeric(data['Valor total em bolsas para o projeto'], errors = 'coerce')
data['Valor total em auxílio financeiro para o projeto'] = pd.to_numeric(data['Valor total em auxílio financeiro para o projeto'], errors = 'coerce')


data = data.dropna()

# selecting data to export 

columns = ['Edital', 
           'Instituição de fomento',
           'Tipo do Projeto',
           'Valor total em bolsas para o projeto',
           'Valor total em auxílio financeiro para o projeto']

data_export = data[columns]


# connecting to PostgreSQL Database 

conn = psycopg2.connect(
    host = "localhost",
    database = "inovacao",
    user = "postgres",
    password="admin"
)

# Create a cursor object

cur = conn.cursor()


# getting column names from the database table

get_cols_sql = f"SELECT * FROM {TABLE} LIMIT 0"
cur.execute(get_cols_sql)


column_names = [desc[0] for desc in cur.description]
columns = ", ".join(column_names)

# creating placeholders
num_cols = len(columns.split(","))
placeholders = ", ".join(["%s"] * num_cols)


# Exporting data with SQL statement 
values = [tuple(x) for x in data_export.to_numpy()]


export_data_sql = f"INSERT INTO {TABLE} ({columns}) VALUES ({placeholders})"
cur.executemany(export_data_sql, values)



# commiting SQL statements

conn.commit()
conn.close()























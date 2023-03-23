#!/usr/bin/env python
'''
python -m venv .venv
pip3 install boto3
pip3 install pandas
pip3 install openpyxl
'''
import boto3
import pandas
from sys import exit
__version__ = '1.0.0'

# Query
query = "SELECT * FROM database_name.table_name"

def query_timestream(query):
    try:
        client = boto3.client('timestream-query')
        return client.query(QueryString=query)
    except Exception as e:
        print('Error when calling AWS Timestream.')
        print(e)
        exit(1)

def process_data(query):
    results = query_timestream(query)
    status_code = results['ResponseMetadata']['HTTPStatusCode']
    request_id = results['ResponseMetadata']['RequestId']

    if status_code == 200:
        rows = results['Rows']
        columns = results['ColumnInfo']
        data = {column['Name']: [] for column in columns}

        len_rows = len(rows)
        if len_rows > 0:
            for row in rows:
                for i, data_point in enumerate(row['Data']):
                    column = columns[i]['Name']
                    data[column].append(list(data_point.values())[0])
            return data
        else:
            print(f'no results from query: \n\t{query}')
            exit(0)
    else: 
        print('error!') # Implementar
        exit(1)
        
def save_csv(filename, data):
    df = pandas.DataFrame(data)
    df.to_excel(filename, index=False)

def main():
    data = process_data(query)
    save_csv('dump.xlsx', data)
    

if __name__ == "__main__":
    main()
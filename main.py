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
__version__ = '2.0.0'

# Query
query = "SELECT * FROM database.table ORDER BY time ASC"

def query_timestream(query, next_token=None):
    try:
        client = boto3.client('timestream-query')
        if next_token != None:
            print('NextToken Found!!! Running query with NextToken')
            return client.query(
                QueryString=query,
                NextToken=f'{next_token}'
                )
        else:
            print('NextToken NotFound: Running query without NextToken')
            return client.query(
                QueryString=query)
    except Exception as e:
        print('Error when calling AWS Timestream.')
        print(e)
        exit(1)

# def unload_database(next_token):


def process_data(query):
    next_token = None
    data = {}
    while True:
        if next_token == None:
            results = query_timestream(query)
        else:
            results = query_timestream(query, next_token)
        status_code = results['ResponseMetadata']['HTTPStatusCode']
        # request_id = results['ResponseMetadata']['RequestId']

        if status_code == 200:
            rows = results['Rows']
            columns = results['ColumnInfo']
            if len(data) == 0:
                data = {column['Name']: [] for column in columns}

            len_rows = len(rows)
            if len_rows > 0:
                for row in rows:
                    for i, data_point in enumerate(row['Data']):
                        column = columns[i]['Name']
                        data[column].append(list(data_point.values())[0])
            if 'NextToken' in results:
                next_token = results['NextToken']
            else:
                return data
        
def save_csv(filename, data):
    df = pandas.DataFrame(data)
    df.to_excel(filename, index=False)

def main():
    data = process_data(query)
    save_csv('dump.xlsx', data)
    

if __name__ == "__main__":
    main()
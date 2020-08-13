import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Column, String, Integer, Date, Table
import os
import sys


def get_dates():
    dates = []
    dates_resp = requests.get('https://xsolla-test-analytics.herokuapp.com/dates', params={
        'token': 'plcOgcreJML2O35Xv0Z6fn3zPMRFtYmX5RzxlXfW5rMVPAIRVrbp9dvWvA843aqr',
    })
    if dates_resp.status_code == 200:
        dates = dates_resp.json()['dates']
    return dates


def get_meetings(day):
    meetings = []
    meetings_resp = requests.get('https://xsolla-test-analytics.herokuapp.com/daily', params={
        'token': 'plcOgcreJML2O35Xv0Z6fn3zPMRFtYmX5RzxlXfW5rMVPAIRVrbp9dvWvA843aqr',
        'day': day,
    })
    if meetings_resp.status_code == 200:
        meetings.extend(meetings_resp.json()['meetings'])
    return meetings


def create_table(engine):
    meta = MetaData()
    meetings_table = Table(
        'r_urakhov', meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', Date),
        Column('bizdev', String),
        Column('company', String),
        Column('category', String)
    )
    meta.create_all(engine)
    return meetings_table


def main():
    # Check for already downloaded data
    if os.path.isfile(f'./meetings.csv'):
        print('Reading a csv file...')
        df_meetings = pd.read_csv('meetings.csv', index_col=0)
    else:
        print('Downloading data via API...')
        dates = get_dates()
        meetings = []
        for day in dates:
            meetings.extend(get_meetings(day))
        df_meetings = pd.DataFrame(meetings)
        print('Csv file "meetings.csv" has been created.')
        df_meetings.to_csv('./meetings.csv')

    # engine = create_engine('postgresql://:'
    #                        'cb8c1a6070ebed38248446e0960ffe7b3fd8ea4927c0aa3ef4d7e1b630588b3f@34.225.162.157:5432/'
    #                        'd6qfh4tmk9h4t2')
    user = 'xqhkevexwktfkk'  # "postgres"
    password = 'cb8c1a6070ebed38248446e0960ffe7b3fd8ea4927c0aa3ef4d7e1b630588b3f'  # "zohan55"
    host = '34.225.162.157'  # "localhost"
    port = '5432'
    db = 'd6qfh4tmk9h4t2'  # "postgres"
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')     

    # Create table
    print('Creating a table...')
    try:
        meetings_table = create_table(engine)
    except Exception as e:
        print(e)
        sys.exit(1)

    # Insert meetings data into database table
    print('Loading meetings data into database table...')
    try:
        with engine.connect() as conn:
            conn.execute(meetings_table.insert(), df_meetings.to_dict(orient='records'))
    except Exception as e:
        print(e)
        sys.exit(1)
    print("Done")


if __name__ == '__main__':
    main()

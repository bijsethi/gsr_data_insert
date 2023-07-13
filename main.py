import requests
import json
import argparse
from datetime import datetime
import psycopg2
import psycopg2.extras

parser = argparse.ArgumentParser(description='Process some Details.')
parser.add_argument('start_date', metavar='start_date', type=str,
                    help='Please provide start date')

parser.add_argument('end_date', metavar='end_date', type=str,
                    help='Please provide end date')

parser.add_argument('dimension', metavar='dimension', type=str,
                    help='Group by description')

args = parser.parse_args()
date_start = args.start_date
date_end = args.end_date
dimension = args.dimension
print(date_start, date_end, dimension)


url = "https://api.libring.com/v2/reporting/get?allow_mock=true"

token = "RVyGynEAbqIfuidTkiYvKdEnn"

# python main.py 2023-06-01 2023-07-01 connection,app,platform,country,date
params = {
    "period": "custom_date",
    "start_date": date_start,
    "end_date": date_end,
    "group_by": dimension,
    "lock_on_collection": "false",
}

response = requests.get(url, params=params, headers={"Authorization": "Token {}".format(token)})

data_response= response.json()


conn = psycopg2.connect(
   database="postgres", user='postgres', password='Postgres@123', host='127.0.0.1', port= '5433'
)
cur = conn.cursor()
# for table creation

# create_extension = """
#     CREATE EXTENSION "uuid-ossp";
    
# """

# table_schema = """

#     CREATE TABLE APP_INFO( 
#     id UUID DEFAULT (uuid_generate_v4()) PRIMARY KEY,
#     CONNECTION VARCHAR,
#     APP VARCHAR,
#     PLATFORM VARCHAR,
#     COUNTRY VARCHAR,
#     DATE DATE,
#     IMPRESSION FLOAT,
#     AD_REVENUE FLOAT)
# """
# cur.execute(create_extension)
# cur.execute(table_schema)
# conn.commit()


data_list=data_response["connections"]

insert_quer="""
    INSERT INTO APP_INFO("connection", "app", "platform", "country", "date", "impression", "ad_revenue") 
    VALUES (%(connection)s, %(app)s, %(platform)s, %(country)s, %(date)s, %(impressions)s, %(ad_revenue)s)
"""

psycopg2.extras.execute_batch(cur, insert_quer, data_list, page_size=100)
conn.commit()

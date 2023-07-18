import traceback
import requests
import json
import argparse
import datetime
import psycopg2
import psycopg2.extras
import os
from config import Config
import pdb

configure = Config()

database_name = configure.database
user_name = configure.user
db_password = configure.password
host_name = configure.host
port_number = configure.port

VALID_DIMENSION = ["connection", "app", "platform", "country", "date"]

class StartDateGreaterThanEndDateError(Exception):
    pass


def db_connection(database, user, password, host, port):
    """
    Establishing a  connection with postgres with :
    Args:
        database (str): "postgres"
        user (str): 'postgres'
        host (str): '127.0.0.1'
        port(str): '5433'
        password(str)
    Returns
        connection Object 
    """
    try:
        conn = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )
        return conn

    except:

        return {"message": "Error Establishing a Database Connection"}


def is_valid_params(date_start, date_end, dimension):
    """
    Checks validity of parameters
    Args
        date_start (date): in YYYY-MM-DD format
        date_end (date): in YYYY-MM-DD format
        dimension (str): connection,app,platform,country,date
    Returns
        True if all parameters are valid else error

    """
    def check_dates():
        try:
            start_date = datetime.date.fromisoformat(date_start)
            end_date = datetime.date.fromisoformat(date_end)
            if start_date > end_date:
                raise StartDateGreaterThanEndDateError("start date cannot be greater than end date!")
        except StartDateGreaterThanEndDateError as e:
            raise ValueError(e)
        except Exception as e:
            raise ValueError(
                f"date_start and  date_end date format, should be YYYY-MM-DD, given dates: start_date: {date_start}, end_date: {date_end}")

    def check_dimension():
        nonlocal dimension
        dimension_l = dimension.split(",")
        for dimension in dimension_l:
            if dimension.strip() not in VALID_DIMENSION:
                raise ValueError(
                    f"dimension value should be combination of: {VALID_DIMENSION}, given dimension: {dimension_l}")

    check_dates()
    check_dimension()
    return True


def main_func():
    
    """Function to insert into database.
        -ArgumentParser to get the input from user with arguments 
        start_date,end_date,dimension
        -validating the parameters with is_valid_params()
        -establishing  connection with db_connection.
        Returns
            {"message": "SUCCESSFULLY INSERTED"}  or {Error}

    """
    try:
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

        is_valid_params(date_start, date_end, dimension)

        params = {
            "period": "custom_date",
            "start_date": date_start,
            "end_date": date_end,
            "group_by": dimension,
            "lock_on_collection": "false",
        }

        conn = db_connection(database_name, user_name, db_password, host_name, port_number)
        cur = conn.cursor()

        response = requests.get(configure.url, params=params,
                                headers={"Authorization": "Token {}".format(configure.url_token)})

        if response.status_code == 200:

            data_response = response.json()

            data_list = data_response["connections"]
            insert_quer = """
                INSERT INTO APP_INFO("connection", "app", "platform", "country", "date", "impression", "ad_revenue") 
                VALUES (%(connection)s, %(app)s, %(platform)s, %(country)s, %(date)s, %(impressions)s, %(ad_revenue)s)
            """
            psycopg2.extras.execute_batch(cur, insert_quer, data_list, page_size=100)
            conn.commit()

            return {"message": "SUCCESSFULLY INSERTED"}

        else:

            return {"message": "BAD REQUEST"}

    except Exception as e:

        return {"message": f"Error: {str(e)}"}


if __name__ == "__main__":
    print(main_func())

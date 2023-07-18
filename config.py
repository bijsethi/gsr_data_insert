import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class Config:
    database = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USERNAME")
    password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT")
    url = os.environ.get("URL")
    url_token = os.environ.get("URL_TOKEN")
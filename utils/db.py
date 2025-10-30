import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv('PGHOST','localhost'),
        port=os.getenv('PGPORT','5432'),
        dbname=os.getenv('PGDATABASE','stocks_id'),
        user=os.getenv('PGUSER','root'),
        password=os.getenv('PGPASSWORD','root'),
    )

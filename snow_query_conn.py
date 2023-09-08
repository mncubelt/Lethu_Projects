import snowflake.connector as sf
import pandas as pd
import os
from dotenv import load_dotenv  # package https://pypi.org/project/python-dotenv/

class SnowflakeQueryExecutor:
    """
    Class to access Snowflake and execute queries
    """
    load_dotenv(".env")
    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = os.getenv("SNOWFLAKE_DATABASE")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA")
        self.role = os.getenv("SNOWFLAKE_ROLE")
        self.conn = self.create_connect()

    #Establish connection
    def create_connect(self):
        conn = sf.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role
        )
        return conn
    
    #Execute query and fetch results
    def execute_query(self, query: str, conn):
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        results_df = pd.DataFrame(result, columns=[x[0] for x in cursor.description])
        return results_df

    #Retrieve list of databases
    def get_list_dbs(self, conn):
        cursor = conn.cursor()
        cursor.execute("SHOW SCHEMAS")
        results = cursor.fetchall()
        databases = [result[1] for result in results if result[5] == 'SYSADMIN']
        return databases
    
    #Retrieve tables within database
    def get_list_db_tbl(self, conn, schema: str, database: str):
        cursor = conn.cursor()
        cursor.execute(f'SHOW TABLES IN "{schema}"."{database}"')
        results = cursor.fetchall()
        tables = [result[1] for result in results]
        return tables


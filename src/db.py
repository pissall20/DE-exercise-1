import os
import sqlite3
import pandas as pd

con = None


def create_connection(db_name='warehouse.db'):
    """
    Function to create connection to SQLite3
    :param db_name: Name of database (default: warehouse.db)
    :return: SQL Connection object
    """
    global con
    # If there an existing connection, then return it without going any further
    # This saves time
    if con:
        return con
    else:
        # Check if warehouse.db exists in directory
        # Even if it doesn't exist, the sqlite3.connect() will create a database
        if db_name in os.listdir():
            print("Database exists in the directory")
        else:
            print("Database does not exist in the directory")
        con = sqlite3.connect(db_name)
        return con


def query_db(database, query=None, nrows=5):
    """
    Generic function to query a database
    :param database: Name of database
    :param query: SQL query to get data from db table
    :param nrows: Number of rows to be fetched
    :return: Returns rows returned by the query
    """
    connection = create_connection(db_name=database)
    cur = connection.cursor()
    if not query:
        query = f'SELECT * from votes' + f" limit {nrows}" if nrows else ""
    cur.execute(query)
    rows = cur.fetchall()
    if not rows:
        print("No results received from query")
        return None
    else:
        return rows


def query_all_rows(db_name, table_name, nrows=None, verbose=False):
    """
    Queries rows from a sqlite db table
    :param db_name: Name of the database
    :param table_name: Name of the table
    :param nrows: Number of rows to be fetched
    :param verbose: Verbosity
    :return: Queried rows
    """
    query = f"SELECT * FROM {table_name}" + f" limit {nrows}" if nrows else ""
    rows = query_db(db_name, query)
    if verbose:
        for row in rows:
            print(row)
    return rows

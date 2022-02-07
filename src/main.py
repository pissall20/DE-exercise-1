import json
import sys
import pandas as pd
import db
import warnings

warnings.filterwarnings('ignore')

debug = False
db_name_demo = 'warehouse.db'


def check_if_data_exists():
    """
    Simple check to see if the data is downloaded or not
    :return: Boolean
    """
    try:
        with open('uncommitted/Posts.json', 'r') as posts_in:
            print(json.load(posts_in)[0])
            print("Data already exists. No need to download.")
            return True
    except FileNotFoundError:
        print("Please download the dataset using 'pipenv run fetch_data'")
        return False


def add_data_from_file(file_path, db_name, table_name, verbose=False):
    """
    Read a file and add data to SQL
    :param file_path: Path of the file to be added
    :param db_name: Name of database
    :param table_name: Name of table
    :param verbose: Verbosity level
    :return: Boolean if data is added or requires to be fetched
    """
    try:
        with open(file_path, 'r') as json_data:
            data = json.load(json_data)
        add_json_data_to_sql(data, db_name, table_name, verbose)
        return True
    except FileNotFoundError:
        print("Please download the dataset using 'pipenv run fetch_data'")
        return False


def mean_votes_per_post_per_week(db_name, table_name='votes'):
    """
    Fetch data and calculate Average number of votes per post per week
    :param db_name: Database name (warehouse.db)
    :param table_name: Table name (votes)
    :return: pd.DataFrame of results
    """
    connection = db.create_connection(db_name)
    df = pd.read_sql_query(
        f"SELECT *, strftime('%W', datetime(CreationDate)) as week  from {table_name}",
        connection)
    # Take the vote count per post grouped by week
    vote_count = df.groupby(['week', 'PostId'])['Id'].count().reset_index()
    # Take mean per week to get an average
    vote_mean = vote_count.groupby(['week'])['Id'].mean().reset_index()
    vote_mean = vote_mean.rename(columns={'Id': 'mean_votes_per_post'})
    return vote_mean


def di_dq_check(df):
    """
    Checks for two qualities:
        - Any duplicates in the primary key of the data
        - If the date column has the correct length for the expected format so that it can be converted
            to datetime smoothly
    :param df: Dataframe to be checked
    :return: Clean and dtype converted dataframe
    """
    unique_id_check = len(df['Id'].unique()) == df.shape[0]
    if not unique_id_check:
        print("There are few duplicate rows.. Dropping them")
        df = df.drop_duplicates(['Id'], keep='first')
        unique_id_check = True
    df.loc[:, 'date_length'] = df['CreationDate'].str.len()
    date_check = (len(df['date_length'].unique()) == 1) and (df['date_length'].unique()[0] == 23)
    if date_check:
        df.loc[:, 'CreationDate'] = pd.to_datetime(df['CreationDate'])
    if unique_id_check and date_check:
        print("Data Quality and Integrity is maintained")
    else:
        print("Data Quality and Integrity is not maintained")
    return df


def add_json_data_to_sql(json_data, database_name, table_name, verbose=False):
    """
    Give some json data or a python dict, add the data to the given database and table using pandas to_sql
    :param json_data: json/dict data to add
    :param database_name: Name of the database
    :param table_name: Name of the table
    :param verbose: Verbosity
    :return: Boolean to notify if data was added or not
    """
    connection = db.create_connection(database_name)
    try:
        print("#" * 130 + f"\nCreating table with name: '{table_name}' in {database_name}..")
        data_df = pd.DataFrame(json_data)
        data_df = di_dq_check(data_df)
        # Add data to the database in mentioned table
        data_df.to_sql(table_name, connection, if_exists='replace', index=False)
        """
        If table already exists, then replace it (this ensures newly added data in the file is also added)
        If we use if_exists='append'
        """

        # Check if data was uploaded
        rows = db.query_all_rows(database_name, table_name, 3, verbose)
        if not rows:
            print("Data is empty or no data was added")
            return False

        # Print some data if verbose=True
        if verbose:
            print("#" * 50 + str(" Printing snippet of data ") + "#" * 50)
            print(data_df)
            print("Existing columns", data_df.columns)
            print("Shape of data is: ", data_df.shape)
            print("#" * 80 + "#" * 80)
        return True
    except Exception as e:
        # If there is a failure to create dataframe, the json data is not in the right format
        print(e)
        print(f"Unable to create table with name: '{table_name}' in {database_name}..\n" + "#" * 130)
        raise ValueError(
            "The JSON data should be a list of dictionaries with same keys")


if __name__ == '__main__':
    try:
        posts_file, votes_file = sys.argv[1], sys.argv[2]

        if check_if_data_exists():
            add_data_from_file(posts_file, db_name_demo, 'posts', debug)
            add_data_from_file(votes_file, db_name_demo, 'votes', debug)

        mean_votes_per_week = mean_votes_per_post_per_week(db_name_demo)
        print("#" * 60 + " what is the mean votes per post per week? " + "#" * 60)
        print(mean_votes_per_week)
        print("#" * 150)
    except IndexError as e:
        print("Error: There should be 2 arguments for the file path of posts and votes file")


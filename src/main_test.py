import pandas as pd

import main
import json

test_db = 'warehouse.db'
test_tbl = 'votes'


def test_mean_votes_per_post_per_week():
    rows_df = main.mean_votes_per_post_per_week(test_db, test_tbl)
    assert len(rows_df) >= 53


def test_check_if_data_exists():
    assert main.check_if_data_exists() in [True, False]


def test_add_data_from_file():
    obj = main.add_data_from_file('uncommitted/Votes.json', test_db, 'example')
    assert obj


def test_di_dq_check():
    votes_df = pd.read_json('uncommitted/Votes.json')
    checked_df = main.di_dq_check(votes_df)
    if isinstance(checked_df, pd.DataFrame) and not checked_df.empty:
        assert True
    else:
        assert False


def test_add_json_data_to_sql():
    with open('uncommitted/Votes.json', 'r') as votes_in:
        votes_data = json.load(votes_in)
    assert main.add_json_data_to_sql(votes_data, test_db, 'test', verbose=False)

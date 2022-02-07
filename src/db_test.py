import sqlite3

import db

test_db = 'warehouse.db'
test_tbl = 'votes'


def test_sqlite3_connection():
    with sqlite3.connect('warehouse.db') as con:
        cursor = con.cursor()
        assert list(cursor.execute('SELECT 1')) == [(1,)]


def test_query_db():
    answer = db.query_db(test_db, 'SELECT * FROM VOTES limit 5')
    assert len(list(answer)) == 5


def test_query_all_rows():
    rows = db.query_all_rows(test_db, test_tbl, nrows=5)
    assert len(rows) == 5


def test_create_connection():
    connection = db.create_connection(test_db)
    assert isinstance(connection, sqlite3.Connection)

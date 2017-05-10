# -*- coding: utf-8 -*-
import re
import psycopg2
from psycopg2 import extras
import json
from exceptions import Exception, AssertionError
import pandas as pd

def cursor_connect(db_name, user_name, password, cursor_factory=None):
    """
    Connects to the DB and returns the connection and cursor, ready to use.

    Parameters
    ----------
    db_dsn : str, unicode
        DSN of the database to connect to.
    cursor_factory : psycopg2.extras
        An optional psycopg2 cursor type, e.g. DictCursor.

    Returns
    -------
    (psycopg2.extensions.connection, psycopg2.extensions.cursor)
        A tuple of (psycopg2 connection, psycopg2 cursor).
    """
    dbname= db_name
    user = user_name
    password= password
    conn = psycopg2.connect(dbname=dbname, user=user, host='localhost', port=5432, password=password)
    cur = conn.cursor()
    
    if not cursor_factory:
        cur = conn.cursor()
    else:
        cur = conn.cursor(cursor_factory=cursor_factory)
    return conn, cur
    

def execute_query(cursor,query):
    """
    Executes SQL query in Postgres SQL database that has been connected to
    
    Parameters
    ----------
    cursor : str, unicode
        cursor returned by cursor connect
    query : str, unicode
        query to pass to Postgres SQL
    
    Returns
    -------
    result: SQL query
        results of query passed to the Postgres SQL connection
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result        

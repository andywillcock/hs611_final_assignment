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
    
def disease_count_by_race(db_name, user_name, password, table_name='cmspop', disease):   
    
    diseases = ('heart_fail','alz_rel_sen','depression','cancer')
    table_names = ('cmspop')
    # Strip the user input to alpha characters only
    cleaned_disease = re.sub('\W+', '', disease)
    try:
        if disease not in diseases:
            raise AssertionError("Disease '{0}' is not allowed".format(cleaned_disease))
        if table_name not in table_names:
            raise AssertionError("Table '{0}' is not allowed please use cmspop or a table with equivalent columns".format(table_name))
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT race, COUNT({1})::integer from {0}
                    WHERE {1} = 't'
                    GROUP BY race;""".format(table_name,cleaned_disease)
        
        result = execute_query(cur, query)
        
        disease_counts = {disease+'_count':[]}
        for row in result:
            count = {'race':row[0], 'count':row[1]}
            disease_counts[disease+'_count'].append(count)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return disease_counts       

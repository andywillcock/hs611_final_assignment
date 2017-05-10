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


def disease_max_carrier_bene_ratio_by_state_sex(db_name, user_name, password, table_name1='cmspop', table_name2='cmsclaims', disease,state):
    """
    Calcualtes the maximum ratio of carrier_reimb/bene_resp and returns the id(s) 
    of the person with that ratio for those in a specified state and having
    a specified disease. 

    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name: str
        table of interest found within db_name
    state : str, unicode
        State abbreviation

    Returns
    -------
    json
        A labeled JSON object with the state and averages for each column value.

    Examples
    --------
    /api/v1/freq/depression
    /api/v1/freq/diabetes
    """        
    diseases = ('heart_fail','alz_rel_sen','depression','cancer')
    states = ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 
        'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 
        'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 
        'VA', 'VT', 'WA', 'WI', 'WV', 'WY', 'Othr')
    
    # Strip the user input to alpha characters only
    cleaned_disease = re.sub('\W+', '', disease)
    if state == 'Othr':
        cleaned_state = 'Othr'
    else:
        cleaned_state = re.sub('\W+', '', state)
        cleaned_state = "'"+cleaned_state.upper()+"'"
    
    # Strip the user input to alpha characters only
    try:
        if disease not in diseases:
            raise AssertionError("Disease '{0}' is not allowed".format(cleaned_disease))
        if state not in states:
             raise AssertionError("State '{0}' is not allowed".format(cleaned_state))
        if table_name1 != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop or a table with equivalent columns".format(table_name1))
        if table_name2 != 'cmsclaims':
            raise AssertionError("Table '{0}' is not allowed please use cmsclaims or a table with equivalent columns".format(table_name2))
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT id, sex, state, MAX(carrier_bene_ratio)::float AS carrier_bene_ratio FROM 
                (SELECT LHS.*, carrier_reimb::float/bene_resp::float AS carrier_bene_ratio FROM
                (SELECT * FROM {0}) AS LHS
                LEFT JOIN
                (SELECT * from {1}) AS RHS
                ON LHS.id = RHS.id 
                WHERE bene_resp > 0 AND {2} = 't' AND state = {3}) as sq1 
		 WHERE carrier_bene_ratio = (SELECT MAX(carrier_bene_ratio)::float AS carrier_bene_ratio FROM 
                (SELECT LHS.*, carrier_reimb::float/bene_resp::float AS carrier_bene_ratio FROM
                (SELECT * FROM {0}) AS LHS
                LEFT JOIN
                (SELECT * from {1}) AS RHS
                ON LHS.id = RHS.id 
                WHERE bene_resp > 0 AND {2} = 't' AND state = {3}) AS sq2)
		 GROUP BY id, sex, state
		 ORDER BY carrier_bene_ratio DESC;""".format(table_name1,table_name2,cleaned_disease,cleaned_state)
        
        result = execute_query(cur, query)
        
        ratios = {'Max_Carrier_Resp/Bene_Resp':[]}
        for row in result:
            ratio = {'id':row[0],'sex':row[1],'state':row[2],'carrier_reimb/bene_resp ratio':row[3]}
            ratios['Max_Carrier_Resp/Bene_Resp'].append(ratio)
            
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
        
    return ratios


                        
def carrier_reimb_avgs_select_state(db_name, user_name, password, table_name='cmspop', state):
    """
    Calculate the state average of carrier reimbursement, hmo months, and beneificiary 
    responsibility for a specified state.

    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name1: str
        table of interest found within db_name
    table_name2: str
        table of interest found within db_name
    state : str, unicode
        State abbreviation

    Returns
    -------
    json
        A labeled JSON object with the state and averages for each column value.

    Examples
    --------
    /api/v1/freq/depression
    /api/v1/freq/diabetes
    """
    
    states = ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 
        'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 
        'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 
        'VA', 'VT', 'WA', 'WI', 'WV', 'WY', 'Othr')
    # Strip the user input to alpha characters only
    if state == 'Othr':
        cleaned_state = 'Othr'
    else:
        cleaned_state = re.sub('\W+', '', state)
        cleaned_state = cleaned_state.upper()
    try:
        if cleaned_state not in states:
            raise AssertionError("State '{0}' is not allowed".format(cleaned_state))
        if table_name1 != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop or a table with equivalent columns".format(table_name1))
        if table_name2 != 'cmsclaims':
            raise AssertionError("Table '{0}' is not allowed please use cmsclaims or a table with equivalent columns".format(table_name2))
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT LHS.state,ROUND(AVG(RHS.carrier_reimb)::numeric,2)::float AS avg_carrier_reimb, 
                ROUND(AVG(RHS.bene_resp)::numeric,2)::float AS avg_bene_resp, 
                ROUND(AVG(RHS.hmo_mo)::numeric,2)::float AS avg_hmo_mo
                FROM                             
                (SELECT * FROM {0}) AS LHS
                LEFT JOIN                           
                (SELECT * FROM {1}) AS RHS
                ON LHS.id = RHS.id
                WHERE state = {2}
                GROUP BY LHS.state;""".format(TABLE_NAME_1,TABLE_NAME_2,"'"+cleaned_state+"'")
        
        result = execute_query(cur, query)
        
        claims_avgs = {'State_Averages':[]}
        
        for row in result:
            freq = {'state':row[0], 'avg_carrier_reimb':row[1], 'avg_bene_resp':row[2], 'avg_hmo_mo':row[2]}
            claims_avgs['State_Averages'].append(freq)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return claims_avgs
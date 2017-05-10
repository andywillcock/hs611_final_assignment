# -*- coding: utf-8 -*-
import re
import psycopg2
from psycopg2 import extras
import json
from exceptions import Exception, AssertionError

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
    
def disease_count_by_race(col, db_name='hs611db', user_name='ATW', password='', table_name='cmspop', category = 'race'):   
    """
    Counts the number of cases of a specified disease for each race

    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name: str
        cmspop or table with identical column names
    col : str
        Boolean variable (Disease of interest for cmspop)
    category: str
        Categorical variable to separate counts by
        Default is race for cmspop table 

    Returns
    -------
    json
        A labeled JSON object with the state and averages for each column value.

    Examples
    --------
    /api/v1/freq/depression
    /api/v1/freq/cancer
    """    
    diseases = ('heart_fail','alz_rel_sen','depression','cancer')
    
    # Strip the user input to alpha characters only
    if table_name == 'cmspop':
        cleaned_disease = re.sub('\W+', '', col)
        try:
            if col not in diseases:
                raise AssertionError("Disease '{0}' is not allowed".format(cleaned_disease))
       
            con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
            query = """SELECT race, COUNT({1})::integer from {0}
                    WHERE {1} = 't'
                    GROUP BY {2};""".format(table_name,cleaned_disease,category)
            result = execute_query(cur, query)
    
            disease_counts = {col+'_count':[]}
            for row in result:
                count = {'race':row[0], 'count':row[1]}
                disease_counts[col+'_count'].append(count)
        except Exception as e:
            raise Exception("Error: {}".format(e.message))
        return disease_counts 
    else:
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT race, COUNT({1})::integer from {0}
                    WHERE {1} = 't'
                    GROUP BY {2};""".format(table_name,col,category)
        
        result = execute_query(cur, query)
        
        disease_counts = {'count':[]}
        for row in result:
            count = {category:row[0], 'count':row[1]}
            disease_counts['count'].append(count)
        return counts       


def disease_max_carrier_bene_ratio_by_state_sex(disease, state, db_name='hs611db', user_name='ATW', password='', table_name1='cmspop', table_name2='cmsclaims'):
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
    table_name1: str
        cmspop or table with identical column names
    table_name2: str
        cmsclaims or table with identical column names
    state : str
        State abbreviation

    Returns
    -------
    json
        A labeled JSON object with the id, sex, state, and max carrier_bene_ratio
        averages for the specified disease.

    Examples
    --------
    /api/v1/max_carrier_bene/depression/WA
    /api/v1/max_carrier_bene/diabetes/CA
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
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name1))
        if table_name2 != 'cmsclaims':
            raise AssertionError("Table '{0}' is not allowed please use cmsclaims".format(table_name2))
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


                        
def carrier_reimb_avgs_select_state(state, db_name='hs611db', user_name='ATW', password='', table_name1='cmspop', table_name2='cmsclaims'):
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
        cmspop or table with identical column names
    table_name2: str
        cmsclaims or table with identical column names
    state : str
        State abbreviation

    Returns
    -------
    json
        A labeled JSON object with the state and averages for each column value.

    Examples
    --------
    /api/v1/carrier_reimb_avg/HI
    /api/v1/carrier_reimb_avg/AL
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
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name1))
        if table_name2 != 'cmsclaims':
            raise AssertionError("Table '{0}' is not allowed please use cmsclaims".format(table_name2))
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
                GROUP BY LHS.state;""".format(table_name1,table_name2,"'"+cleaned_state+"'")
        
        result = execute_query(cur, query)
        
        claims_avgs = {'State_Averages':[]}
        
        for row in result:
            freq = {'state':row[0], 'avg_carrier_reimb':row[1], 'avg_bene_resp':row[2], 'avg_hmo_mo':row[2]}
            claims_avgs['State_Averages'].append(freq)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return claims_avgs
    
def avg_death_age_for_concurrent_disease_by_sex(disease1, disease2, db_name='hs611db', user_name='ATW', password='', table_name='cmspop'):
    """
    Calculates the average age of death (by sex) for those who had at least
    the two specified diseases.
    
    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name1: str
        cmspop or table with identical column names
    disease1 : str
        disease type
    disease2 : str
        disease type
        
    Returns
    -------
    json
        A labeled JSON object with the race and average age of death of those 
        with both diseases.

    Examples
    --------
    /api/v1/avg_age_of_death/depression/heart_fail
    /api/v1/avg_age_of_death/cancer/alz_rel_sen
    """
    
    diseases = ('heart_fail','alz_rel_sen','depression','cancer')
    # Strip the user input to alpha characters only
    cleaned_disease1 = re.sub('\W+', '', disease1)
    cleaned_disease2 = re.sub('\W+', '', disease1)
    try:
        if cleaned_disease1 not in diseases:
            raise AssertionError("Disease {0} is not allowed".format(cleaned_disease1))
        if cleaned_disease2 not in diseases:
            raise AssertionError("Disease {0} is not allowed".format(cleaned_disease2))
        if table_name != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name))
        
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT sex, FLOOR(avg(age)::integer) AS avg_age_of_death 
                FROM (SELECT sex, FLOOR((dod-dob)/365) AS age from {0} WHERE dod IS NOT NULL AND {1} ='t' AND {2} ='t') as sq1 
                GROUP BY sex;""".format(table_name,cleaned_disease1, cleaned_disease2)
        
        result = execute_query(cur, query)
        
        avg_death_ages = {'Average_age_of_death':[]}
        for row in result:
            age = {'sex':row[0],'avg_age_of_death':row[1]}
            avg_death_ages['Average_age_of_death'].append(age)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return avg_death_ages
    
def high_and_low_carrier_reimb_state(race, db_name='hs611db', user_name='ATW', password='', table_name1='cmspop', table_name2='cmsclaims'):
    """
    Get the states with the highest and lowest total carrier reimbursement 
    for a specified race.

    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name1: str
        cmspop or table with identical column names
    table_name2: str
        cmsclaims or table with identical column names
    race : str
        race of persons of interest

    Returns
    -------
    max_min
        A labeled JSON object with the state, race and total carrier 
        reimbursement.

    Examples
    --------
    /api/v1/max_min_carrier_reimb/black
    /api/v1/max_min_carrier_reimb/others
    """
    
    races = ('white','black','hispanic','others')
    
    # Strip the user input to alpha characters only
    cleaned_race = re.sub('\W+', '', race)
    try:
        if cleaned_race not in races:
            raise AssertionError("Race '{0}' is not allowed".format(cleaned_race))
        if table_name1 != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name1))
        if table_name2 != 'cmsclaims':
            raise AssertionError("Table '{0}' is not allowed please use cmsclaims".format(table_name2))
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT state,race,total_carrier_reimb::float 
                FROM( SELECT  LHS.state, LHS.race, SUM(RHS.carrier_reimb) AS total_carrier_reimb FROM
                (SELECT * FROM {1}) AS RHS
                LEFT JOIN
                (SELECT * FROM {0}) AS LHS
                ON LHS.id = RHS.id WHERE race = {2}
                GROUP BY LHS.state,LHS.race) AS sq1
                WHERE total_carrier_reimb = (SELECT MIN(total_carrier_reimb)::float 
                FROM (SELECT SUM(LHS.carrier_reimb) AS total_carrier_reimb, RHS.state, RHS.race FROM
                (SELECT * FROM {1}) AS LHS
                LEFT JOIN
                (SELECT * FROM {0}) AS RHS
                ON LHS.id = RHS.id WHERE race = {2}
                GROUP BY RHS.state, RHS.race) AS sq2
                ) OR total_carrier_reimb = (SELECT MAX(total_carrier_reimb)::float 
                FROM (SELECT SUM(LHS.carrier_reimb) AS total_carrier_reimb, RHS.state, RHS.race FROM
                (SELECT * FROM {1}) AS LHS
                LEFT JOIN
                (SELECT * FROM {0}) AS RHS
                ON LHS.id = RHS.id WHERE race = {2}
                GROUP BY RHS.state, RHS.race) AS sq3)
                ORDER by total_carrier_reimb ASC;""".format(table_name1,table_name2,"'"+cleaned_race+"'")
        
        result = execute_query(cur, query)
        
        total_carrier_reimb = {'Total_Carrier_Reimbursements':[]}
        for row in result:
            min_max = {'state':row[0], 'race':row[1],'carrier_reimb':row[2]}
            total_carrier_reimb['Total_Carrier_Reimbursements'].append(min_max)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return total_carrier_reimb


def max_total_cost_state_status(state, status, db_name='hs611db', user_name='ATW', password='', table_name1='cmspop', table_name2='cmsclaims'):
    """
    Get the id of the person of a specified status (alive or dead) with the 
    greatest total cost (carrier_reimb + bene_resp) in  a specified state. 


    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name1: str
        cmspop or table with identical column names
    table_name2: str
        cmsclaims or table with identical column names
    state : str
        state if interest
    status : str
        person's alive or dead status

    Returns
    -------
    max_total_cost
        A labeled JSON object with the id, state, status, and total cost

    Examples
    --------
    /api/v1/total_cost/MA/dead
    /api/v1/total_cost/OR/alive
    """
    states = ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 
        'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 
        'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 
        'VA', 'VT', 'WA', 'WI', 'WV', 'WY', 'Othr')
    statuses = ('alive','dead')
    
    # Strip the user input to alpha characters only
    cleaned_status = re.sub('\W+', '', status)
    if state == 'Othr':
        cleaned_state = 'Othr'
    else:
        cleaned_state = re.sub('\W+', '', state)
        cleaned_state = cleaned_state.upper()
    try:
        if cleaned_state not in states:
            raise AssertionError("Race '{0}' is not allowed".format(cleaned_state))
        if cleaned_status not in statuses:
            raise AssertionError("Race '{0}' is not allowed".format(cleaned_status))
        if table_name1 != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name1))
        if table_name2 != 'cmsclaims':
            raise AssertionError("Table '{0}' is not allowed please use cmsclaims".format(table_name2))    
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT id, state,status, total_cost 
                FROM (SELECT LHS.id, LHS.state,RHS.carrier_reimb+RHS.bene_resp AS total_cost, LHS.status 
                FROM (SELECT id,state,status 
                FROM (SELECT *,CASE 
                WHEN dod IS NOT NULL THEN 'dead'
                WHEN dod IS NULL THEN 'alive'
                END AS status
                FROM {0}) as sq1) AS LHS
                LEFT JOIN
                (SELECT * from {1}) AS RHS
                ON LHS.id = RHS.id WHERE state = {2} AND status = '{3}') as sq2 
                WHERE total_cost = (SELECT max(total_cost) 
                FROM (SELECT LHS.id, LHS.state,RHS.carrier_reimb+RHS.bene_resp AS total_cost, LHS.status 
                FROM (SELECT id,state,status 
                FROM (SELECT *,CASE 
                WHEN dod IS NOT NULL THEN 'dead'
                WHEN dod IS NULL THEN 'alive'
                END AS status
                FROM {0}) as sq1) AS LHS
                LEFT JOIN
                (SELECT * from {1}) AS RHS
                ON LHS.id = RHS.id WHERE state = {2} and status = '{3}') as sq2)  ;""".format(table_name1,table_name2,"'"+cleaned_state+"'",cleaned_status)
        
        result = execute_query(cur, query)
        
        max_total_cost = {'Max_Total_Cost':[]}
        
        for row in result:
            cost = {'id':row[0], 'state':row[1], 'status':row[2],'total_cost':row[3]}
            max_total_cost['Max_Total_Cost'].append(cost)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return max_total_cost
    
    
def hmo_mo_gt_average_for_state_disease(state, disease, db_name='hs611db', user_name='ATW', password='', table_name1='cmspop', table_name2='cmsclaims'):
    """
    Returns the rows with hmo_mo values for those with a chosen disease greater 
    than the average hmo_mo value for that sample.

    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name1: str
        cmspop or table with identical column names
    table_name2: str
        cmsclaims or table with identical column names
    state : str
        state of interest
    disease : str
        disease of interest

    Returns
    -------
    gt_average
        A labeled JSON object with the id, state, disease diagnosis, and hmo_mo

    Examples
    --------
    /api/v1/gt_hmo_avg/CO/depression
    /api/v1/gt_hmo_avg/AK/cancer
    """
    states = ('AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 
        'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 
        'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 
        'VA', 'VT', 'WA', 'WI', 'WV', 'WY', 'Othr')
    diseases = ('heart_fail','alz_rel_sen','depression','cancer')
    # Strip the user input to alpha characters only
    if state == 'Othr':
        cleaned_state = 'Othr'
    else:
        cleaned_state = re.sub('\W+', '', state)
        cleaned_state = cleaned_state.upper()
    cleaned_disease = re.sub('\W+', '', disease)
    try:
        if cleaned_state not in states:
            raise AssertionError("Race '{0}' is not allowed".format(cleaned_state))
        if cleaned_disease not in diseases:
            raise AssertionError("Race '{0}' is not allowed".format(cleaned_disease))    
        if table_name1 != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name1))
        if table_name2 != 'cmsclaims':
            raise AssertionError("Table '{0}' is not allowed please use cmsclaims".format(table_name2))
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT id, state, {3},hmo_mo 
                FROM (SELECT LHS.id,state,{3},hmo_mo  
                FROM (SELECT * FROM {0}) AS LHS
                LEFT JOIN                                     
                (SELECT * from {1}) AS RHS
                ON LHS.id = RHS.id WHERE state = {2} AND {3} = 't') as sq1
                WHERE hmo_mo > (SELECT avg(hmo_mo) AS avg_hmo_mo 
                FROM (SELECT LHS.id, state, cancer, hmo_mo  
                FROM (SELECT * FROM {0}) AS LHS
                LEFT JOIN                                     
                (SELECT * from {1}) AS RHS
                ON LHS.id = RHS.id WHERE state = {2} AND {3} = 't')as sq2);""".format(table_name1,table_name2,"'"+cleaned_state+"'",cleaned_disease)
        
        result = execute_query(cur, query)
        
        gt_average = {'Greater_Than_Average_HMO_MO':[]}
        
        for row in result:
            gt_avg = {'id':row[0], 'state':row[1], cleaned_disease:row[2],'hmo_mo':row[3]}
            gt_average['Greater_Than_Average_HMO_MO'].append(gt_avg)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return gt_average  
    
def state_avg_life_expectancies_by_sex(state, db_name='hs611db', user_name='ATW', password='', table_name='cmspop'): 
    """
    Returns the average life expectancies for each sex for a chosen state for 
    people with none of the diseases (healthy) compared to those with one of the four
    diseases and only that disease (For example: Those who only have 
    cancer = 't' (depression, heart_fail, and alz_rel_sen all = 'f')

    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name1: str
        cmspop or table with identical column names
    state : str
        state if interest

    Returns
    -------
    life_expectancies
        A labeled JSON object with the state, sex, healthy life expectancy,
        alzheimers life expectancy, heart failure life expectancy, depression 
        life expectancy, and cancer life expectancy.

    Examples
    --------
    /api/v1/avg_life_expectancy/AZ
    /api/v1/avg_life_expectancy/TX
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
        if table_name != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name1))
    
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT LHS4.state, LHS4.sex, avg_healthy_life_expectancy, avg_alzheimers_life_expectancy, avg_hf_life_expectancy, avg_depression_life_expectancy, avg_cancer_life_expectancy FROM 
                (SELECT LHS3.state, LHS3.sex, avg_healthy_life_expectancy, avg_alzheimers_life_expectancy, avg_hf_life_expectancy, avg_depression_life_expectancy FROM 
                (SELECT LHS2.state, LHS2.sex, avg_healthy_life_expectancy, avg_alzheimers_life_expectancy, avg_hf_life_expectancy FROM 
                (SELECT LHS.state, LHS.sex, avg_healthy_life_expectancy, avg_alzheimers_life_expectancy FROM
                (SELECT state, sex, FLOOR(avg(age))::integer as avg_healthy_life_expectancy 
                FROM (SELECT state, sex, (dod-dob)/365 AS age from {0} WHERE dod IS NOT NULL AND alz_rel_sen = 'f' AND cancer = 'f' AND heart_fail = 'f' AND depression = 'f') AS sq1 
                GROUP BY sex, state) AS LHS
                LEFT JOIN
                (SELECT state, sex, FLOOR(avg(age))::integer as avg_alzheimers_life_expectancy FROM (SELECT state, sex, (dod-dob)/365 AS age from {0} WHERE dod IS NOT NULL AND alz_rel_sen = 't' AND cancer = 'f' AND heart_fail = 'f' AND depression = 'f') AS sq2 
                GROUP BY sex,state) AS RHS
                ON LHS.state = RHS.state AND LHS.sex = RHS.sex) AS LHS2
                LEFT JOIN 
                (SELECT state, sex, FLOOR(avg(age))::integer as avg_hf_life_expectancy
                FROM (SELECT state, sex, (dod-dob)/365 AS age from {0} WHERE dod IS NOT NULL AND heart_fail = 't' AND alz_rel_sen = 'f' AND cancer = 'f' AND depression = 'f') AS sq2 
                GROUP BY sex,state) AS RHS2
                ON LHS2.state = RHS2.state AND LHS2.sex = RHS2.sex) AS LHS3
                LEFT JOIN
                (SELECT state, sex, FLOOR(avg(age))::integer as avg_depression_life_expectancy
                FROM (SELECT state, sex, (dod-dob)/365 AS age from {0} WHERE dod IS NOT NULL AND depression = 't' AND alz_rel_sen = 'f' AND cancer = 'f' AND heart_fail = 'f') AS sq2 
                GROUP BY sex,state) AS RHS3
                ON LHS3.state = RHS3.state AND LHS3.sex = RHS3.sex) AS LHS4
                LEFT JOIN
                (SELECT state, sex, FLOOR(avg(age))::integer as avg_cancer_life_expectancy
                FROM (SELECT state, sex, (dod-dob)/365 AS age from {0} WHERE dod IS NOT NULL AND cancer= 't' AND alz_rel_sen = 'f' AND heart_fail = 'f' AND depression = 'f' ) AS sq2 
                GROUP BY sex,state) AS RHS4
                ON LHS4.state = RHS4.state AND LHS4.sex = RHS4.sex
                WHERE LHS4.state = {1};""".format(table_name,"'"+cleaned_state+"'")
        
        result = execute_query(cur, query)
        
        life_expectancies = {'Life_Expectancies':[]}
        for row in result:
            expect = {'state':row[0], 'sex':row[1], 'avg healthy life expectancy':row[2],'avg alzheimers life expectancy':row[3],'avg heart failure life expectancy':row[4],'avg depression life expectancy':row[5],'avg cancer life expectancy':row[6]}
            life_expectancies['Life_Expectancies'].append(expect)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return life_expectancies  

def claims_deviations_by_state(state, db_name='hs611db', user_name='ATW', password='', table_name1='cmspop', table_name2='cmsclaims'): 
    """
    Get the deviations from (the mean of) carrier_reimnb, bene_resp, and
    hmo_mo in the specified state.

    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name1: str
        cmspop or table with identical column names
    table_name2: str
        cmsclaims or table with identical column names
    state : str
        state if interest

    Returns
    -------
    deviations
        A labeled JSON object with the id, state, deviation from carrier_reimb
        mean, deviation from bene_resp mean, and deviation from hmo_mo mean.

    Examples
    --------
    /api/v1/deviations/MD
    /api/v1/deviations/NC
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
            raise AssertionError("Race '{0}' is not allowed".format(cleaned_state))   
        if table_name1 != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name1))
        
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        query = """SELECT id, state, ROUND(carrier_reimb-(SELECT AVG(carrier_reimb) as avg_carrier FROM (SELECT LHS.id,LHS.state,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * from {0}) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq1 
                WHERE state = {2})::numeric,2)::float AS carrier_deviation, ROUND(bene_resp-(SELECT AVG(bene_resp) as avg_bene FROM (SELECT LHS.id,LHS.state,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * from {0}) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq2
                WHERE state = {2})::numeric,2)::float AS bene_deviation, ROUND(hmo_mo-(SELECT AVG(hmo_mo) as avg_hmo FROM (SELECT LHS.id,LHS.state,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * from {0}) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq3
                WHERE state = {2})::numeric,2)::float AS hmo_deviation FROM (SELECT LHS.id,LHS.state,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * from {0}) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq4
                WHERE state = {2}
                ORDER BY carrier_deviation;""".format(table_name1,table_name2,"'"+cleaned_state+"'")
        
        result = execute_query(cur, query)
        
        deviations = {'deviations':[]}
        
        for row in result:
            patient = {'id':row[0], 'state':row[1], 'carrier_reimb deviation':row[2],'bene_resp deviation':row[3],'homo_mo deviation':row[4]}
            deviations['deviations'].append(patient)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return deviations

def stat_select_for_sex(stat, sex,db_name='hs611db', user_name='ATW', password='', table_name1='cmspop', table_name2='cmsclaims'):
    """
    Calculates/returns the selected statistcal measure of age, carrier_reimnb, 
    bene_resp, and hmo_mo for a specified sex.

    Parameters
    ----------
    db_name: str
        name of database being accessed
    user_name: str
        username used to access the specfied database
    password: str
        password corresponding to user_name
    table_name1: str
        cmspop or table with identical column names
    table_name2: str
        cmsclaims or table with identical column names
    stat : str
        statistical measurement of interest
    sex: str
        sex for the statistic to be calculated for

    Returns
    -------
    stat_dict
        A labeled JSON object with the sex and specified statistic for age,
        carrier_reimb, bene_resp, and hmo_mo.

    Examples
    --------
    /api/v1/stats/median/male
    /api/v1/stats/sd/female
    """
    stats = ('mean','median','sd')
    sexes = ('male','female')
    
    # Strip the user input to alpha characters only
    cleaned_stat = re.sub('\W+', '', stat)
    cleaned_sex = re.sub('\W+', '', sex)
    try:
        if cleaned_stat not in stats:
            raise AssertionError("Statistic '{0}' is not allowed".format(cleaned_stat)) 
        if cleaned_sex not in sexes:
            raise AssertionError("Sex '{0}' is not allowed".format(cleaned_sex)) 
        if table_name1 != 'cmspop':
            raise AssertionError("Table '{0}' is not allowed please use cmspop".format(table_name1))
        if table_name2 != 'cmsclaims':
            raise AssertionError("Table '{0}' is not allowed please use cmsclaims".format(table_name2))
            
        con, cur = cursor_connect(db_name, user_name, password, cursor_factory=None)
        
        if stat == 'mean':
            query = """ SELECT sex, FLOOR(avg(age)) AS age, ROUND(avg(carrier_reimb)::numeric,2)::float AS avg_carrier_resp, ROUND(avg(bene_resp)::numeric,2)::float AS avg_bene_resp, ROUND(avg(hmo_mo)::numeric,2)::float AS avg_hmo_mo FROM (SELECT LHS.id,LHS.sex,LHS.state,FLOOR((LHS.dod-dob)/365) AS age, RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                    (SELECT * FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                    ON LHS.id=RHS.id WHERE sex = {2}) AS tbl1
                    GROUP by sex;""".format(table_name1,table_name2, "'"+cleaned_sex+"'")
        if stat == 'median':
                query = """SELECT sex, FLOOR(median_age)::float AS median_age,ROUND(median_carrier_reimb,2)::float AS median_carrier_reimb, ROUND(median_bene_resp,2)::float AS median_bene_resp,ROUND(median_hmo_mo,2)::float AS median_hmo_mo  FROM (
                    (WITH med_age AS (SELECT age, row_number() OVER (ORDER BY age) AS row_id,
                    (SELECT COUNT(1) FROM (SELECT *, (dod-dob)/365 AS age FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                    ON LHS.id=RHS.id WHERE  sex =  {2}) AS ct
                    FROM
                    (SELECT *, (dod-dob)/365 AS AGE FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                        ON LHS.id=RHS.id WHERE  sex =  {2})
                    SELECT AVG(age) AS median_age
                    FROM med_age
                    WHERE row_id BETWEEN ct/2.0 AND ct/2.0 + 1) AS t0
                    
                    CROSS JOIN
                    
                    (WITH med_carrier_reimb AS (SELECT carrier_reimb, row_number() OVER (ORDER BY carrier_reimb) AS row_id,
                            (SELECT count(1) FROM (SELECT * FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                    ON LHS.id=RHS.id WHERE  sex =  {2}) AS ct
                    FROM 
                    (SELECT * FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                    ON LHS.id=RHS.id WHERE sex =  {2})
                    SELECT avg(carrier_reimb) AS median_carrier_reimb
                    FROM med_carrier_reimb
                    WHERE row_id BETWEEN ct/2.0 AND ct/2.0 + 1) AS t1
                    
                    CROSS JOIN
                    
                    (WITH med_bene_resp AS (SELECT bene_resp, row_number() OVER (ORDER BY bene_resp) AS row_id,
                    (SELECT count(1) FROM (SELECT * FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                    ON LHS.id=RHS.id WHERE  sex =  {2}) AS ct
                    FROM 
                    (SELECT * FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                    ON LHS.id=RHS.id WHERE  sex =  {2})
                    select avg(bene_resp) AS median_bene_resp
                    FROM med_bene_resp
                    WHERE row_id between ct/2.0 and ct/2.0 + 1) AS t2
                    
                    CROSS JOIN
                    
                    (WITH med_hmo_mo AS (SELECT sex, hmo_mo, row_number() OVER (ORDER BY hmo_mo) AS row_id,
                    (SELECT count(1) FROM (SELECT * FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                    ON LHS.id=RHS.id WHERE  sex =  {2}) AS ct
                    FROM 
                    (SELECT * FROM {0} WHERE dod IS NOT NULL) AS LHS
                    LEFT JOIN
                    (SELECT * FROM {1}) AS RHS
                    ON LHS.id=RHS.id WHERE  sex =  {2})
                    SELECT  sex, avg(hmo_mo) AS median_hmo_mo
                    FROM med_hmo_mo
                    WHERE row_id BETWEEN ct/2.0 AND ct/2.0 + 1
                    GROUP BY sex) AS t3) AS meds;
                    """.format(table_name1,table_name2,"'"+cleaned_sex+"'")
        if stat == 'sd':
            query = """SELECT * FROM
                (SELECT sex, ROUND(SQRT(SUM(ROUND(age-(SELECT AVG(age) AS avg_age FROM (SELECT LHS.id,LHS.sex,LHS.age,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT *, (dod-dob)/365 AS age FROM {0} WHERE dod IS NOT NULL) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq1 
                WHERE sex = {2})::numeric,2)::float^2)/COUNT(sex))::numeric,2)::float AS age_sd FROM 
		(SELECT LHS.id,LHS.sex,LHS.age,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT *, (dod-dob)/365 AS age FROM {0} WHERE dod IS NOT NULL) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq4
		WHERE sex = {2} GROUP BY sex) AS t0
		
                CROSS JOIN 
                
                (SELECT ROUND(SQRT(SUM(ROUND(carrier_reimb-(SELECT AVG(carrier_reimb) AS avg_carrier FROM (SELECT LHS.id,LHS.sex,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * FROM {0}) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq1 
                WHERE sex = {2})::numeric,2)::float^2)/COUNT(sex))::numeric,2)::float AS carrier_sd FROM (SELECT LHS.id,LHS.sex,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * FROM {0}) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq4
		WHERE sex = {2} GROUP BY sex) AS t1
		
                CROSS JOIN 
                
                (SELECT ROUND(SQRT(SUM(ROUND(bene_resp-(SELECT AVG(bene_resp) AS avg_bene FROM (SELECT LHS.id,LHS.sex,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * FROM {0}) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq1 
                WHERE sex = {2})::numeric,2)::float^2)/COUNT(sex))::numeric,2)::float AS bene_sd FROM (SELECT LHS.id,LHS.sex,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * FROM {0}) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq4
		 WHERE sex = {2} GROUP BY sex) AS t2
		 
                CROSS JOIN 
                
                (SELECT ROUND(SQRT(SUM(ROUND(bene_resp-(SELECT AVG(hmo_mo) AS avg_bene FROM (SELECT LHS.id,LHS.sex,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * FROM cmspop) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq1 
                WHERE sex = {2})::numeric,2)::float^2)/COUNT(sex))::numeric,2)::float AS hmo_mo_sd FROM (SELECT LHS.id,LHS.sex,RHS.carrier_reimb,RHS.bene_resp,RHS.hmo_mo FROM
                (SELECT * FROM cmspop) AS LHS
                LEFT JOIN
                (SELECT * FROM {1}) AS RHS
                ON LHS.id=RHS.id) AS sq4
		 WHERE sex = {2} GROUP BY sex) AS t3;""".format(table_name1,table_name2,"'"+cleaned_sex+"'")
        
        result = execute_query(cur, query)
        
        stat_dict = {'statistic':[]}
        
        if stat == 'mean':
            for row in result:
                statistic = {'sex':row[0], 'age':row[1], 'mean_carrier_reimb':row[2],'mean_bene_resp':row[3],'mean_homo_mo devations':row[4]}
                stat_dict['statistic'].append(statistic)
        if stat == 'median':
            for row in result:
                statistic = {'sex':row[0], 'age':row[1], 'median_carrier_reimb':row[2],'median_bene_resp':row[3],'median_homo_mo devations':row[4]}
                stat_dict['statistic'].append(statistic)
        if stat == 'sd':
            for row in result:
                statistic = {'sex':row[0], 'age':row[1], 'carrier_reimb_sd':row[2],'bene_resp_sd':row[3],'homo_mo_sd':row[4]}
                stat_dict['statistic'].append(statistic)        
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return stat_dict

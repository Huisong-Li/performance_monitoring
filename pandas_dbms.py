#!/usr/bin/python
# -*- coding: utf-8 -*-
# ------------------------------------
# author:https://github.com/catawbasam
#-------------------------------------


"""
LICENSE: BSD (same as pandas)
example use of pandas with oracle mysql postgresql sqlite
    - updated 9/18/2012 with better column name handling; couple of bug fixes.
    - used ~20 times for various ETL jobs.  Mostly MySQL, but some Oracle. 
    to do:  
            save/restore index (how to check table existence? just do select count(*)?), 
            finish odbc, 
            add booleans?, 
"""

import numpy as np
import cStringIO
import pandas.io.sql as psql
from dateutil import parser

dbtypes={'oracle': {'DATE':'DATE', 'DATETIME':'DATE',               'INT':'NUMBER',  'FLOAT':'NUMBER', 'VARCHAR':'VARCHAR2'}}

# from read_frame.  ?datetime objects returned?  convert to datetime64?
def read_db(sql, con):
    return psql.frame_query(sql, con)


def table_exists(name=None, con=None, flavor='sqlite'):
    if flavor == 'sqlite':
        sql="SELECT name FROM sqlite_master WHERE type='table' AND name='MYTABLE';".replace('MYTABLE', name)
    elif flavor == 'mysql':
        sql="show tables like 'MYTABLE';".replace('MYTABLE', name)
    elif flavor == 'postgresql':
        sql= "SELECT * FROM pg_tables WHERE tablename='MYTABLE';".replace('MYTABLE', name)
    elif flavor == 'oracle':
        sql="select table_name from user_tables where table_name='MYTABLE'".replace('MYTABLE', name.upper())
    elif flavor == 'odbc':
        raise NotImplementedError
    else:
        raise NotImplementedError
    
    df = read_db(sql, con)
    print sql, df
    print 'table_exists?', len(df)
    exists = True if len(df)>0 else False
    return exists

def write_frame(frame, name=None, con=None, flavor='sqlite', if_exists='fail'):
    """
    Write records stored in a DataFrame to specified dbms. 
    
    if_exists:
        'fail'    - create table will be attempted and fail
        'replace' - if table with 'name' exists, it will be deleted        
        'append'  - assume table with correct schema exists and add data.  if no table or bad data, then fail.
            ??? if table doesn't exist, make it.
        if table already exists.  Add: if_exists=('replace','append','fail')
    """

    if if_exists=='replace' and table_exists(name, con, flavor):    
        cur = con.cursor()   
        cur.execute("drop table "+name)
        cur.close()    
    
    if if_exists in ('fail','replace') or ( if_exists=='append' and table_exists(name, con, flavor)==False ):
        #create table
        schema = get_schema(frame, name, flavor)
        if flavor=='oracle':
            schema = schema.replace(';','')
        cur = con.cursor()    
        if flavor=='mysql':
            cur.execute("SET sql_mode='ANSI_QUOTES';")
        print 'schema\n', schema
        cur.execute(schema)
        cur.close()
        print 'created table' 
        
    cur = con.cursor()
    #bulk insert

        
    if  flavor=='oracle':
        cols=[db_colname(k) for k in frame.dtypes.index]
        colnames = ','.join(cols)
        colpos = ', '.join([':'+str(i+1) for i,f in enumerate(cols)])
        insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (name, colnames, colpos)
        #print 'insert_sql', insert_sql
        data = [ convertSequenceToDict(rec) for rec in frame.values] 
        #print data
        cur.executemany(insert_sql, data)

    else:
        raise NotImplementedError        
    con.commit()
    cur.close()
    return

def nan2none(df):
    dnp = df.values
    for rw in dnp:
        rw2 = tuple([ None if v==np.Nan else v for v in rw])
        
    tpl_list= [ tuple([ None if v==np.Nan else v for v in rw]) for rw in dnp ] 
    return tpl_list
    
def db_colname(pandas_colname):
    '''convert pandas column name to a DBMS column name
        TODO: deal with name length restrictions, esp for Oracle
    '''
    colname =  pandas_colname.replace(' ','_').strip()                  
    return colname
    

    # append data into existing postgresql table using COPY
    
    # 1. convert df to csv no header
    output = cStringIO.StringIO()
    
    # deal with datetime64 to_csv() bug
    have_datetime64 = False
    dtypes = df.dtypes
    for i, k in enumerate(dtypes.index):
        dt = dtypes[k]
        print 'dtype', dt, dt.itemsize
        if str(dt.type)=="<type 'numpy.datetime64'>":
            have_datetime64 = True

    if have_datetime64:
        d2=df.copy()    
        for i, k in enumerate(dtypes.index):
            dt = dtypes[k]
            if str(dt.type)=="<type 'numpy.datetime64'>":
                d2[k] = [ v.to_pydatetime() for v in d2[k] ]                
        #convert datetime64 to datetime
        #ddt= [v.to_pydatetime() for v in dd] #convert datetime64 to datetime
        d2.to_csv(output, sep='\t', header=False, index=False)
    else:
        df.to_csv(output, sep='\t', header=False, index=False)                        
    output.seek(0)
    contents = output.getvalue()
    print 'contents\n', contents
       
    # 2. copy from
    cur = con.cursor()
    cur.copy_from(output, name)    
    con.commit()
    cur.close()
    retur

#source: http://www.gingerandjohn.com/archives/2004/02/26/cx_oracle-executemany-example/
def convertSequenceToDict(list):
    """for  cx_Oracle:
        For each element in the sequence, creates a dictionary item equal
        to the element and keyed by the position of the item in the list.
        >>> convertListToDict(("Matt", 1))
        {'1': 'Matt', '2': 1}
    """
    dict = {}
    argList = range(1,len(list)+1)
    for k,v in zip(argList, list):
        dict[str(k)] = v
    return dict

    
def get_schema(frame, name, flavor):
    types = dbtypes[flavor]  #deal with datatype differences
    column_types = []
    dtypes = frame.dtypes
    for i,k in enumerate(dtypes.index):
        dt = dtypes[k]
        #print 'dtype', dt, dt.itemsize
        if str(dt.type)=="<type 'numpy.datetime64'>":
            sqltype = types['DATETIME']
        elif issubclass(dt.type, np.datetime64):
            sqltype = types['DATETIME']
        elif issubclass(dt.type, (np.integer, np.bool_)):
            sqltype = types['INT']
        elif issubclass(dt.type, np.floating):
            sqltype = types['FLOAT']
        else:
            sampl = frame[ frame.columns[i] ][0]
            #print 'other', type(sampl)    
            if str(type(sampl))=="<type 'datetime.datetime'>":
                sqltype = types['DATETIME']
            elif str(type(sampl))=="<type 'datetime.date'>":
                sqltype = types['DATE']                   
            else:
                if flavor in ('mysql','oracle'):                
                    size = 2 + max( (len(str(a)) for a in frame[k]) )
                    print k,'varchar sz', size
                    sqltype = types['VARCHAR'] + '(?)'.replace('?', str(size) )
                else:
                    sqltype = types['VARCHAR']
        colname =  db_colname(k)  #k.upper().replace(' ','_')                  
        column_types.append((colname, sqltype))
    columns = ',\n  '.join('%s %s' % x for x in column_types)
    template_create = """CREATE TABLE %(name)s (
                      %(columns)s
                    );"""    
    #print 'COLUMNS:\n', columns
    create = template_create % {'name' : name, 'columns' : columns}
    return create
    

###############################################################################


def test_oracle(name, testdf):
    print '\nOracle'
    import cx_Oracle
    with cx_Oracle.connect('perfdata/perfdata123@192.168.4.10:1522/orcl') as ora_conn:
        #testdf['d64'] = np.datetime64( testdf['hire_date'] )
        write_frame(testdf, name, con=ora_conn, flavor='oracle', if_exists='replace')    
        df_ora2 = read_db('select * from '+name, con=ora_conn)    

    print 'done with oracle'
    return df_ora2



##############################################################################

if __name__=='__main__':

    from pandas import DataFrame
    from datetime import datetime
    
    print """Aside from sqlite, you'll need to install the driver and set a valid
            connection string for each test routine."""
    
    test_data = {
        "name": [ 'Joe', 'Bob', 'Jim', 'Suzy', 'Cathy', 'Sarah' ],
        "hire_date": [ datetime(2012,1,1), datetime(2012,2,1), datetime(2012,3,1), datetime(2012,4,1), datetime(2012,5,1), datetime(2012,6,1) ],
        "erank": [ 1,   2,   3,   4,   5,   6  ],
        "score": [ 1.1, 2.2, 3.1, 2.5, 3.6, 1.8]
    }
    df = DataFrame(test_data)

    name='test_df'

    test_oracle(name, df)
     
    
    print 'done'
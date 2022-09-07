import streamlit as st
st.title("snowflake data app")

import pandas as pd
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake22!',
                account='cy06007.ap-southeast-1',
                warehouse='MAX',
                database='SNOWFLAKE',
                schema='ACCOUNT_USAGE',
    ocsp_fail_open=False
                )
db = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where deleted is NULL and database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn)
dbs = list(set(list(db['DATABASE'])))
option = st.selectbox('select database:',dbs)
st.write('You selected:', option)

sc= pd.read_sql("select schema_name as schema,catalog_name as database from SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA where deleted is NULL and catalog_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn) 
sc.loc[sc['DATABASE']==option] 
scs = list(set(list(sc['SCHEMA'])))
next = st.selectbox('select schema:',scs)
st.write('You selected:', next)
tab = pd.read_sql("select table_name as table,table_schema as schema,table_catalog as database from SNOWFLAKE.ACCOUNT_USAGE.TABLES where deleted is NULL and table_catalog not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn)
tab.loc[tab['SCHEMA']==next]





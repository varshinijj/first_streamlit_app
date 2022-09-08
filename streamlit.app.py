import streamlit as st
st.title("Snowflake Data-App")

import pandas as pd
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake@22!',
                account='bv18063.ap-southeast-1',
    ocsp_fail_open=False
                )
    
db = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where deleted is NULL and database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn)
dbs = list(set(list(db['DATABASE'])))
option = st.selectbox('select database:',dbs)
st.write('Selected Database :', option)

sc= pd.read_sql("select CATALOG_NAME AS DATABASE,SCHEMA_NAME AS SCHEMA from {}.information_schema.SCHEMATA where SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA');".format(option),conn)
sc
scs = list(set(list(sc['SCHEMA'])))
next = st.selectbox('select schema:',scs)
st.write('Selected Schema:', next)

tab = pd.read_sql("select TABLE_CATALOG AS DATABASE,TABLE_SCHEMA AS SCHEMA,TABLE_NAME from {}.information_schema.TABLES where TABLE_SCHEMA = {};".format(option,next),conn) 
tab
tabs = list(set(list(tabl2['TABLE_NAME'])))
final = st.selectbox('select table:',tabs)
st.write('Selected Table:', final)



val = pd.read_sql("select COLUMN_NAME,TAG_NAME,TAG_VALUE from table({}.information_schema.tag_references_all_columns('{}.{}.{}', 'table'));".format(option,option,next,final),conn)
val = val.pivot(index=['COLUMN_NAME'],columns=['TAG_NAME'],values=['TAG_VALUE']).reset_index()
val






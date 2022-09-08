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

sc= pd.read_sql("select schema_name as schema,catalog_name as database from SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA where deleted is NULL and catalog_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn) 
scl = sc.loc[sc['DATABASE']==option].reset_index(drop=True)
scl
scs = list(set(list(scl['SCHEMA'])))
next = st.selectbox('select schema:',scs)
st.write('Selected Schema:', next)

tab = pd.read_sql("select table_name,table_schema as schema,table_catalog as database from SNOWFLAKE.ACCOUNT_USAGE.TABLES where deleted is NULL and table_catalog not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn)
tabl1 = tab.loc[tab['SCHEMA']==next]
tabl2 = tabl1.loc[tabl1['DATABASE']==option].reset_index(drop=True)
tabl2
tabs = list(set(list(tabl2['TABLE_NAME'])))
final = st.selectbox('select table:',tabs)
st.write('Selected Table:', final)

dis = pd.read_sql("select COLUMN_NAME,TAG_NAME,TAG_VALUE,OBJECT_NAME from snowflake.account_usage.tag_references;",conn)
disp = dis.loc[dis['OBJECT_NAME']==final][['COLUMN_NAME','TAG_NAME','TAG_VALUE']].reset_index(drop=True)
disp_pivot=disp.pivot(index=['COLUMN_NAME'],columns=['TAG_NAME'],values=['TAG_VALUE']).reset_index()
disp_pivot







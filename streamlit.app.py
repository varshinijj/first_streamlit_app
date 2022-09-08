import streamlit as st
import plotly.graph_objects as go
st.sidebar.title("CONFIGURE WAREHOUSE")

import pandas as pd
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake@22!',
                account='bv18063.ap-southeast-1',
                warehouse = 'UI',
    ocsp_fail_open=False
                )
st.sidebar.markdown("click here to resize warehouse")
size = st.sidebar.selectbox('select size', 
                                    ('XSMALL','SMALL','MEDIUM','LARGE','XLARGE','2XLARGE','3XLARGE','4XLARGE'),index=3)
  
apply = st.sidebar.checkbox("Apply all changes", False, key = 1)
if apply:
  conn.cursor().execute("alter warehouse UI set warehouse_size = {};".format(size))
else:
  conn.cursor().execute("alter warehouse UI set warehouse_size = LARGE;")

    
db = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where deleted is NULL and database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn)
dbs = list(set(list(db['DATABASE'])))
option = st.selectbox('select database:',dbs)
st.write('Selected Database :', option)

sc= pd.read_sql("select CATALOG_NAME AS DATABASE,SCHEMA_NAME AS SCHEMA from {}.information_schema.SCHEMATA where SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA');".format(option),conn)
sc
scs = list(set(list(sc['SCHEMA'])))
next = st.selectbox('select schema:',scs)
st.write('Selected Schema:', next)

tab = pd.read_sql("select TABLE_CATALOG AS DATABASE,TABLE_SCHEMA AS SCHEMA,TABLE_NAME from {}.information_schema.TABLES;".format(option),conn) 
tab = tab.loc[tab['SCHEMA']==next]
tab
tabs = list(set(list(tab['TABLE_NAME'])))
final = st.selectbox('select table:',tabs)
st.write('Selected Table:', final)

conn.cursor().execute("call ASSOCIATE_SEMANTIC_CATEGORY_TAGS('{}.{}.{}',EXTRACT_SEMANTIC_CATEGORIES('{}.{}.{}'))".format(option,next,final,option,next,final));



val = pd.read_sql("select COLUMN_NAME,TAG_NAME,TAG_VALUE from table({}.information_schema.tag_references_all_columns('{}.{}.{}', 'table'));".format(option,option,next,final),conn)
val = val.pivot(index=['COLUMN_NAME'],columns=['TAG_NAME'],values=['TAG_VALUE']).reset_index()
val






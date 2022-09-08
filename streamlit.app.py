import streamlit as st
import plotly.graph_objects as go
st.sidebar.title("Configure Warehouse")

import pandas as pd
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake@22!',
                account='bv18063.ap-southeast-1',
                warehouse = 'UI',
    ocsp_fail_open=False
                )

size = st.sidebar.selectbox('select size', ('XSMALL','SMALL','MEDIUM','LARGE','XLARGE','2XLARGE','3XLARGE','4XLARGE'),'LARGE')
min, max = st.sidebar.select_slider('Select min and max clusters',options=['1', '2', '3', '4', '5', '6', '7','8','9','10'],value=('1', '2'))
st.sidebar.write('min:', int(min), 'max:', int(max))
  
apply = st.sidebar.checkbox("Apply", False, key = 1)
if apply:
  conn.cursor().execute("alter warehouse UI set warehouse_size ={} MAX_CLUSTER_COUNT ={} MIN_CLUSTER_COUNT ={};".format(size,int(max),int(min)))
else:
  conn.cursor().execute("alter warehouse UI set warehouse_size = LARGE MAX_CLUSTER_COUNT =2 MIN_CLUSTER_COUNT =1;")

    
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






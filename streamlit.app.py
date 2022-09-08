import streamlit as st
import plotly.graph_objects as go
import graphviz as graphviz
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

size = st.sidebar.selectbox('select size', ('XSMALL','SMALL','MEDIUM','LARGE','XLARGE','2XLARGE','3XLARGE','4XLARGE'),3)
min, max = st.sidebar.select_slider('Select min and max clusters',options=['1', '2', '3', '4', '5', '6', '7','8','9','10'],value=('1', '2'))
st.sidebar.write('min:', int(min), 'max:', int(max))
  
apply = st.sidebar.checkbox("Apply", False, key = 1)
if apply:
  conn.cursor().execute("alter warehouse UI set warehouse_size ={} MAX_CLUSTER_COUNT ={} MIN_CLUSTER_COUNT ={};".format(size,int(max),int(min)))
else:
  conn.cursor().execute("alter warehouse UI set warehouse_size = LARGE MAX_CLUSTER_COUNT =2 MIN_CLUSTER_COUNT =1;")

    
db_data = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where deleted is NULL and database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn)
dbs = list(set(list(db_data['DATABASE'])))
st.sidebar.title("Choose Database")
DB = st.sidebar.selectbox('select database:',dbs)
tab = pd.read_sql("select TABLE_CATALOG AS DATABASE,TABLE_SCHEMA AS SCHEMA,TABLE_NAME from {}.information_schema.TABLES where TABLE_SCHEMA != 'INFORMATION_SCHEMA';".format(DB),conn) 
tab




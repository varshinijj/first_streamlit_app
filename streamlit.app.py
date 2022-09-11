import streamlit as st
import plotly.graph_objects as go
import graphviz as graphviz
import pandas as pd
st.set_page_config(layout="wide")
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake@22!',
                account='bg35464.ap-southeast-1',
                warehouse = 'SQLWH',
                ocsp_fail_open=False)


db_data = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA') and deleted is null;",conn)
dbs = list(set(list(db_data['DATABASE'])))
st.sidebar.title("Choose Database")
DB = st.sidebar.radio('select database:',dbs,0)

st.sidebar.title("Configure Warehouse")
size = st.sidebar.selectbox('select size', ('XSMALL','SMALL','MEDIUM','LARGE','XLARGE','2XLARGE','3XLARGE','4XLARGE'),1)
min, max = st.sidebar.select_slider('Select min and max clusters',options=['1', '2', '3', '4', '5', '6', '7','8','9','10'],value=('1', '2'))
st.sidebar.write('min:', int(min), 'max:', int(max))
apply = st.sidebar.checkbox("Apply", False, key = 1)
if apply:
  conn.cursor().execute("alter warehouse SQLWH set warehouse_size ={} MAX_CLUSTER_COUNT ={} MIN_CLUSTER_COUNT ={};".format(size,int(max),int(min)))
else:
  conn.cursor().execute("alter warehouse SQLWH set warehouse_size = SMALL MAX_CLUSTER_COUNT =2 MIN_CLUSTER_COUNT =1;")

    
sc = pd.read_sql("select CATALOG_NAME AS DATABASE,SCHEMA_NAME AS SCHEMA from {}.information_schema.SCHEMATA where SCHEMA_NAME !='INFORMATION_SCHEMA';".format(DB),conn)
sc_tb = pd.read_sql("select TABLE_SCHEMA AS SCHEMA,TABLE_NAME from {}.information_schema.TABLES where TABLE_SCHEMA != 'INFORMATION_SCHEMA';".format(DB),conn)
col1, col2,col3 = st.columns([1, 6,1])

with col1:
  select = ['All Schemas','Select Schemas']
  click = st.radio('Choose Schemas:',select)
  if click =='All Schemas':
    sc =  pd.read_sql("select CATALOG_NAME AS DATABASE,SCHEMA_NAME AS SCHEMA from {}.information_schema.SCHEMATA where SCHEMA_NAME !='INFORMATION_SCHEMA';".format(DB),conn)
    sc_tb = pd.read_sql("select TABLE_SCHEMA AS SCHEMA,TABLE_NAME from {}.information_schema.TABLES where TABLE_SCHEMA != 'INFORMATION_SCHEMA';".format(DB),conn)
  else:
    for x in list(sc['SCHEMA']):
      schemas = st.checkbox('{}'.format(x),False)
      if schemas==False:
        sc = sc.loc[sc['SCHEMA']!=x]
        sc_tb = sc_tb.loc[sc_tb['SCHEMA']!=x]
  classify = st.button('Classify')
  if classify:
    for idx,row in sc_tb.iterrows():
      row['TABLE_NAME']


   
with col2:
  
  d = graphviz.Digraph()
  d.attr(bgcolor='black')
  with d.subgraph() as s:
    s.attr(rank='same')
    s.node('{}'.format(DB), fontcolor='white',color = 'white')  
  with d.subgraph() as s:
    s.attr(rank='same')
    for x in list(sc['SCHEMA']):
      s.node('{}'.format(x), fontcolor='white',color = 'white')
      d.edge('{}'.format(DB),'{}'.format(x),headlabel='Schema',labelfontcolor='white', len='1.00',color='white') 
  with d.subgraph() as s:
    s.attr(rank='same')
    for idx,row in sc_tb.iterrows():
      s.node('{}'.format(row['TABLE_NAME']),shape='tab', fontcolor='white',color = 'white')
      d.edge('{}'.format(row['SCHEMA']),'{}'.format(row['TABLE_NAME']),color='white')
     
             
       
  st.graphviz_chart(d)

with col3:
  st.subheader("masking policy options")
  



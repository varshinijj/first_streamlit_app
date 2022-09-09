import streamlit as st
import plotly.graph_objects as go
import graphviz as graphviz
import pandas as pd
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake@22!',
                account='bg35464.ap-southeast-1',
                ocsp_fail_open=False)

db_data = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA') and deleted is null;",conn)
dbs = list(set(list(db_data['DATABASE'])))
st.sidebar.title("Choose Database")
DB = st.sidebar.selectbox('select database:',dbs)

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
col1, col2 = st.columns([1, 4])

with col1:
    for x in list(sc['SCHEMA'].unique()):
        schemas = st.checkbox('{}'.format(x),False)
        if schemas==False:
          sc = sc.loc[sc['SCHEMA']!=x]
          sc_tb = sc_tb.loc[sc_tb['SCHEMA']!=x]
    allschemas = st.checkbox('All schemas',True)
    if allschemas:
      sc =  pd.read_sql("select CATALOG_NAME AS DATABASE,SCHEMA_NAME AS SCHEMA from {}.information_schema.SCHEMATA where SCHEMA_NAME !='INFORMATION_SCHEMA';".format(DB),conn)
      sc_tb = pd.read_sql("select TABLE_SCHEMA AS SCHEMA,TABLE_NAME from {}.information_schema.TABLES where TABLE_SCHEMA != 'INFORMATION_SCHEMA';".format(DB),conn)  
tags = pd.read_sql("select OBJECT_DATABASE as database,OBJECT_SCHEMA as schema,OBJECT_NAME as table_name,COLUMN_NAME,TAG_NAME,TAG_VALUE FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES where object_deleted is null;",conn) 
tags1 = tags.loc[tags['DATABASE']==DB][['SCHEMA','TABLE_NAME','COLUMN_NAME','TAG_NAME','TAG_VALUE']]

tags_semantic = tags1.loc[tags1['TAG_NAME']=='SEMANTIC_CATEGORY'][['COLUMN_NAME','TAG_VALUE']]

tags_tb = tags1.pivot(index=['SCHEMA','TABLE_NAME','COLUMN_NAME'],columns=['TAG_NAME'],values=['TAG_VALUE']).reset_index()
with col2:
  d = graphviz.Digraph()
  with d.subgraph() as s:
    s.attr(rank='same')
    s.node('{}'.format(DB)) 
  with d.subgraph() as s:
    s.attr(rank='same')
    for x in list(sc['SCHEMA']):
      s.node('{}'.format(x))
      d.edge('{}'.format(DB),'{}'.format(x)) 
  with d.subgraph() as s:
    s.attr(rank='same')
    for idx,row in sc_tb.iterrows():
      s.node('{}'.format(row['TABLE_NAME']))
      d.edge('{}'.format(row['SCHEMA']),'{}'.format(row['TABLE_NAME']))
  with d.subgraph() as s:
    s.attr(rank='same')
    for idx,row in tags_tb.iterrows():
      s.node('{}'.format(str(row['COLUMN_NAME']).split()[1]))
      d.edge('{}'.format(str(row['TABLE_NAME']).split()[1]),'{}'.format(str(row['COLUMN_NAME']).split()[1]))
  with d.subgraph() as s:
    s.attr(rank='same',shape='diamond')
    for idx,row in tags_semantic.iterrows():
      s.node('{}'.format(row['TAG_VALUE']))
      d.edge('{}'.format(row['COLUMN_NAME']),'{}'.format(row['TAG_VALUE']))
       
  st.graphviz_chart(d)





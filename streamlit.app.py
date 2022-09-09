import streamlit as st
import plotly.graph_objects as go
import graphviz as graphviz
import pandas as pd
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake@22!',
                account='bv18063.ap-southeast-1',
                warehouse = 'UI',
    ocsp_fail_open=False
                )

db_data = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where deleted is NULL and database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn)
dbs = list(set(list(db_data['DATABASE'])))
st.sidebar.title("Choose Database")
DB = st.sidebar.selectbox('select database:',dbs)

st.sidebar.title("Configure Warehouse")
size = st.sidebar.selectbox('select size', ('XSMALL','SMALL','MEDIUM','LARGE','XLARGE','2XLARGE','3XLARGE','4XLARGE'),3)
min, max = st.sidebar.select_slider('Select min and max clusters',options=['1', '2', '3', '4', '5', '6', '7','8','9','10'],value=('1', '2'))
st.sidebar.write('min:', int(min), 'max:', int(max))
apply = st.sidebar.checkbox("Apply", False, key = 1)
if apply:
  conn.cursor().execute("alter warehouse UI set warehouse_size ={} MAX_CLUSTER_COUNT ={} MIN_CLUSTER_COUNT ={};".format(size,int(max),int(min)))
else:
  conn.cursor().execute("alter warehouse UI set warehouse_size = SMALL MAX_CLUSTER_COUNT =2 MIN_CLUSTER_COUNT =1;")

    
sc = pd.read_sql("select CATALOG_NAME AS DATABASE,SCHEMA_NAME AS SCHEMA from {}.information_schema.SCHEMATA where SCHEMA_NAME !='INFORMATION_SCHEMA';".format(DB),conn)
sc_tb = pd.read_sql("select TABLE_SCHEMA AS SCHEMA,TABLE_NAME from {}.information_schema.TABLES where TABLE_SCHEMA != 'INFORMATION_SCHEMA';".format(DB),conn)
tags = pd.read_sql("select OBJECT_DATABASE as database,OBJECT_SCHEMA as schema,OBJECT_NAME as table_name,COLUMN_NAME,TAG_NAME,TAG_VALUE FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES where object_deleted is null;",conn) 
tags_tb = tags.loc[tags['DATABASE']==DB][['SCHEMA','TABLE_NAME','COLUMN_NAME','TAG_NAME','TAG_VALUE']]

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
        s.node('{}'.format(row['COLUMN_NAME']))
        d.edge('{}'.format(row['TABLE_NAME']),'{}'.format(row['COLUMN_NAME']))
     
             

        
 
st.graphviz_chart(d)





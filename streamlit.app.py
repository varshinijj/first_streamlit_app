import streamlit as st
import plotly.graph_objects as go
import graphviz as graphviz
import pandas as pd
import snowflake.connector
st.set_page_config(layout="wide")

####connecting to snowflake account####

conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake@22!',
                account='bg35464.ap-southeast-1',
                warehouse = 'SQLWH',
                ocsp_fail_open=False)


####database selection####

@st.experimental_singleton
def all_databases():
  db_data = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA') and deleted is null;",conn)
  dbs = list(set(list(db_data['DATABASE'])))
  return dbs
st.sidebar.title("Choose Database")
DB = st.sidebar.radio('Available databases:',all_databases())

####warehouse configuration####

st.sidebar.title("Configure Warehouse")
size = st.sidebar.selectbox('select size', ('XSMALL','SMALL','MEDIUM','LARGE','XLARGE'),1)
min, max = st.sidebar.select_slider('Select min and max clusters',options=['1', '2', '3', '4', '5', '6', '7','8','9','10'],value=('1', '2'))
st.sidebar.write('min:', int(min), 'max:', int(max))
apply = st.sidebar.button("Apply")
if apply:
  conn.cursor().execute("alter warehouse SQLWH set warehouse_size ={} MAX_CLUSTER_COUNT ={} MIN_CLUSTER_COUNT ={};".format(size,int(max),int(min)))
else:
  conn.cursor().execute("alter warehouse SQLWH set warehouse_size = SMALL  MAX_CLUSTER_COUNT =2 MIN_CLUSTER_COUNT =1;")

####schemas and tables in the database are queried####   

sc = pd.read_sql("select CATALOG_NAME AS DATABASE,SCHEMA_NAME AS SCHEMA from {}.information_schema.SCHEMATA where SCHEMA_NAME !='INFORMATION_SCHEMA';".format(DB),conn)
sc_tb = pd.read_sql("select TABLE_SCHEMA AS SCHEMA,TABLE_NAME from {}.information_schema.TABLES where TABLE_SCHEMA != 'INFORMATION_SCHEMA';".format(DB),conn)



####separating layout into 3 columns####

tab1, tab2 = st.tabs(["Detailed view",  "overview"])

####col1--selecting schemas, classifying and if classified---removing the tags option####
with tab1:
  col1, col2 = st.columns([8,2])
  with col1:
  
####selecting schemas####

    select = ['All Schemas','Select Schemas']
    click = st.radio('Choose Schema:',select,key=2,horizontal=True)
    if click =='All Schemas':
      pass
    else:
      for x in list(sc['SCHEMA']):
        schemas = st.checkbox('{}'.format(x),False)
        if schemas==False:
          sc = sc.loc[sc['SCHEMA']!=x]
          sc_tb = sc_tb.loc[sc_tb['SCHEMA']!=x] 
      
####Classifying tables in schemas selected and applying tags on columns####

    classify = st.button('Classify')
    if classify:
      if sc.shape[0] ==0:
        st.error('A Schema has to be selected', icon="🚨")
      else:
        alltags = pd.DataFrame(columns=['SCHEMA', 'TABLE_NAME', 'COLUMN_NAME','TAG_NAME','TAG_VALUE'])
        alldatatypes = pd.DataFrame(columns=['DATABASE','SCHEMA', 'TABLE_NAME', 'COLUMN_NAME','DATA_TYPE'])
        for idx,row in sc_tb.iterrows():
          conn.cursor().execute("call ASSOCIATE_SEMANTIC_CATEGORY_TAGS('{}.{}.{}',EXTRACT_SEMANTIC_CATEGORIES('{}.{}.{}'));".format(DB,row['SCHEMA'],row['TABLE_NAME'],DB,row['SCHEMA'],row['TABLE_NAME']))        
          tags = pd.read_sql("select OBJECT_SCHEMA as schema,OBJECT_NAME as table_name,COLUMN_NAME,TAG_NAME,TAG_VALUE from table({}.information_schema.tag_references_all_columns('{}.{}.{}','table'));".format(DB,DB,row['SCHEMA'],row['TABLE_NAME']),conn)         
          datatype = pd.read_sql("select TABLE_CATALOG as database,TABLE_SCHEMA as schema,TABLE_NAME,COLUMN_NAME ,DATA_TYPE  FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA ='{}' and TABLE_NAME = '{}';".format(DB,row['SCHEMA'],row['TABLE_NAME']),conn)
          alltags = alltags.append(tags, ignore_index=True) 
          alldatatypes = alldatatypes.append(datatype,ignore_index=True)
        st.success('Tags Applied!', icon="✅")  
        tags_pivot = alltags.pivot(index=['SCHEMA','TABLE_NAME','COLUMN_NAME'],columns=['TAG_NAME'],values=['TAG_VALUE']).reset_index()
        tags_tb = tags_pivot[['SCHEMA','TABLE_NAME']]
        tags_tb_grouped = tags_tb.groupby(['SCHEMA','TABLE_NAME']).size().reset_index(name='no.of.sensitive_col')
        alldatatypes = alldatatypes.rename(columns = {'TABLE_NAME':'TABLE NAME','ÇOLUMN_NAME':'COLUMN NAME','DATA_TYPE':'DATA TYPE'})
####Removing the applied tags from tables####

        st.write("click to remove tags")
        remove = st.button('Remove')
        if remove:
          for idx,row in tags_pivot:
            conn.cursor().execute("alter table {}.{}.{} modify column {} unset tag {};".format(DB,row['SCHEMA'],row['TABLE_NAME'],row['COLUMN_NAME'],row['TAG_NAME']))
            tags = pd.read_sql("select OBJECT_SCHEMA as schema,OBJECT_NAME as table_name,COLUMN_NAME,TAG_NAME,TAG_VALUE from table({}.information_schema.tag_references_all_columns('{}.{}.{}','table'));".format(DB,DB,row['SCHEMA'],row['TABLE_NAME']),conn) 
            alltags = alltags.append(tags, ignore_index=True)
          st.success('Tags Removed!', icon="✅")    
          tags_pivot = alltags.pivot(index=['SCHEMA','TABLE_NAME','COLUMN_NAME'],columns=['TAG_NAME'],values=['TAG_VALUE']).reset_index()
          tags_tb = tags_pivot[['SCHEMA','TABLE_NAME']]
          tags_tb_grouped = tags_tb.groupby(['SCHEMA','TABLE_NAME']).size().reset_index(name='no.of.sensitive_col')
   
####tab2--graphical representation of database,schemas,tables and if classified ---number of sensitive columns and the tags on each column displayed####

with tab2: 
  st.info('The number of sensitive columns are shown after classification', icon="ℹ️")
  
####graphical representation of database,schemas,tables####  
  
  d = graphviz.Digraph()
  d.attr(bgcolor='#0e1117',overlap='False')
  with d.subgraph() as s:
    s.attr(rank='same')
    s.node('{}'.format(DB), fontcolor='white',color = 'white')  
  with d.subgraph() as s:
    s.attr(rank='same')
    for x in list(sc['SCHEMA']):
      s.node('{}'.format(x), fontcolor='white',color = 'white')
      d.edge('{}'.format(DB),'{}'.format(x),headlabel='Schema',labelfontcolor='white', len='1.00',color='white') 
  with d.subgraph() as s:
    if classify==False:
      sc_tb = sc_tb.sort_values(['SCHEMA','TABLE_NAME']).reset_index()
      sl = []
      for idx,row in sc_tb.iterrows():
        if row['SCHEMA'] not in sl:
          df= sc_tb.loc[sc_tb['SCHEMA']==row['SCHEMA']][['TABLE_NAME']]
          df = df.reset_index(drop=True)
          df.rename(columns = {'TABLE_NAME':'TABLES'}, inplace = True)
          s.node('{}'.format(df),shape='tab', fontcolor='white',color = 'white')
          d.edge('{}'.format(row['SCHEMA']),'{}'.format(df),color='white')
          sl.append(row['SCHEMA'])  
    else:
      
####number of tags in each table####      
      tl =[]
      for idx,row in tags_tb_grouped.iterrows():
        if row['SCHEMA'] not in tl:
          df= tags_tb_grouped.loc[tags_tb_grouped['SCHEMA']==row['SCHEMA']][['TABLE_NAME','no.of.sensitive_col']]
          df = df.reset_index(drop=True)
          df.rename(columns = {'TABLE_NAME':'TABLES','no.of.sensitive_col':'SENSITIVE_COLS'}, inplace = True)
          s.node('{}'.format(df),shape='tab', fontcolor='white',color = 'white')
          d.edge('{}'.format(row['SCHEMA']),'{}'.format(df),color='white')
          tl.append(row['SCHEMA'])  
                   

####graph displayed####
          
  if sc.shape[0] ==0:
    st.write("**Please Select a Schema**")
  else:
    st.graphviz_chart(d)
  
####tags on each column displayed in tabular format####
with tab1:
  with col1:
    if sc.shape[0] ==0:
      pass
    else:
      if classify==True:
        display=pd.merge(sc,tags_pivot, on=['SCHEMA'], how='inner').rename(columns={('TABLE_NAME',''):'TABLE NAME',('COLUMN_NAME',''):'COLUMN NAME',('TAG_VALUE','SEMANTIC_CATEGORY'):'SEMANTIC CATEGORY',('TAG_VALUE','PRIVACY_CATEGORY'):'PRIVACY CATEGORY'})
        
        final = pd.merge(display,alldatatypes,left_on=['DATABASE','SCHEMA','TABLE NAME','COLUMN NAME'],right_on=['DATABASE','SCHEMA','TABLE NAME','COLUMN_NAME'], how = 'left')
        final      
####col3---masking policy options####  
with tab1:
  with col2:
    st.write("masking policy options")
  
  
  
  



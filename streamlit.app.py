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
cur = conn.cursor()
####database selection####
@st.experimental_singleton
def all_databases():
  db_data = pd.read_sql("select database_name as database from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA') and deleted is null;",conn)
  dbs = list(set(list(db_data['DATABASE'])))
  return dbs
st.sidebar.title("Choose Database")
DB = st.sidebar.radio('Databases to classify:',all_databases())
####warehouse configuration####
st.sidebar.title("Configure Warehouse")
size = st.sidebar.selectbox('select size', ('XSMALL','SMALL','MEDIUM','LARGE','XLARGE'),1)
min, max = st.sidebar.select_slider('Select min and max clusters',options=['1', '2', '3', '4', '5', '6', '7','8','9','10'],value=('1', '2'))
st.sidebar.write('min:', int(min), 'max:', int(max))
apply = st.sidebar.button("Apply")
if apply:
  conn.cursor().execute("alter warehouse <> set warehouse_size ={} MAX_CLUSTER_COUNT ={} MIN_CLUSTER_COUNT ={};".format(size,int(max),int(min)))
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
    
    
    if sc_tb.shape[0]!=0:     
      click2 = st.radio('TABLES',['All Tables','Select Tables'],key=3,horizontal=True) 
      if click2 =='All Tables':
        pass
      else:
        tables = st.multiselect('',list(sc_tb['TABLE_NAME']),key=4)
        tables = (str(tables)[1:-1])
        for n in list(sc_tb['TABLE_NAME']):
          if n not in tables:
            sc_tb = sc_tb.loc[sc_tb['TABLE_NAME']!=n] 
            
####Classifying tables in schemas selected and applying tags on columns####
@st.cache
def convert_df(df):
  return df.to_csv().encode('utf-8')
  
with tab1:
  with col1:
    if sc_tb.shape[0]!=0:
      alltags = pd.DataFrame(columns=['SCHEMA', 'TABLE_NAME', 'COLUMN_NAME','TAG_NAME','TAG_VALUE'])
      alldatatypes = pd.DataFrame(columns=['DATABASE','SCHEMA', 'TABLE_NAME', 'COLUMN_NAME','DATA_TYPE'])
      for idx,row in sc_tb.iterrows():
        conn.cursor().execute("call ASSOCIATE_SEMANTIC_CATEGORY_TAGS('{}.{}.{}',EXTRACT_SEMANTIC_CATEGORIES('{}.{}.{}'));".format(DB,row['SCHEMA'],row['TABLE_NAME'],DB,row['SCHEMA'],row['TABLE_NAME']))        
        tags = pd.read_sql("select OBJECT_SCHEMA as schema,OBJECT_NAME as table_name,COLUMN_NAME,TAG_NAME,TAG_VALUE from table({}.information_schema.tag_references_all_columns('{}.{}.{}','table'));".format(DB,DB,row['SCHEMA'],row['TABLE_NAME']),conn)         
        datatype = pd.read_sql("select TABLE_CATALOG as database,TABLE_SCHEMA as schema,TABLE_NAME,COLUMN_NAME ,DATA_TYPE  FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA ='{}' and TABLE_NAME = '{}';".format(DB,row['SCHEMA'],row['TABLE_NAME']),conn)
        alltags = alltags.append(tags, ignore_index=True) 
        alldatatypes = alldatatypes.append(datatype,ignore_index=True)
      if alltags.shape[0]!=0:  
        tags_pivot = alltags.pivot(index=['SCHEMA','TABLE_NAME','COLUMN_NAME'],columns=['TAG_NAME'],values=['TAG_VALUE']).reset_index()
        tags_tb = tags_pivot[['SCHEMA','TABLE_NAME']]
        tags_tb_grouped = tags_tb.groupby(['SCHEMA','TABLE_NAME']).size().reset_index(name='no.of.sensitive_col')
        alldatatypes = alldatatypes.rename(columns = {'TABLE_NAME':'TABLE NAME','ÇOLUMN_NAME':'COLUMN NAME','DATA_TYPE':'DATA TYPE'})
        display=pd.merge(sc,tags_pivot, on=['SCHEMA'], how='inner').rename(columns={('TABLE_NAME',''):'TABLE NAME',('COLUMN_NAME',''):'COLUMN NAME',('TAG_VALUE','SEMANTIC_CATEGORY'):'SEMANTIC CATEGORY',('TAG_VALUE','PRIVACY_CATEGORY'):'PRIVACY CATEGORY'})
        final = pd.merge(display,alldatatypes,left_on=['DATABASE','SCHEMA','TABLE NAME','COLUMN NAME'],right_on=['DATABASE','SCHEMA','TABLE NAME','COLUMN_NAME'], how = 'left').drop(['COLUMN_NAME'],axis=1)
        final = final[['DATABASE','SCHEMA','TABLE NAME','COLUMN NAME','DATA TYPE','PRIVACY CATEGORY','SEMANTIC CATEGORY']]
        st.write("Classification")
        final
        csv = convert_df(final)
        st.download_button("Export Report",data=csv,file_name='Tags.csv',mime='text/csv')
 
      else:
        st.info('No columns in any of the tables has any sensitive data', icon="ℹ️")
    elif sc.shape[0]!=0:
      if sc_tb.shape[0]!=0:
        if click2=='Select Tables':
          st.info('Please Select a Table', icon="ℹ️")
        else:
          st.info('No Tables under the schema', icon="ℹ️")
    elif sc_tb.shape[0]!=0 and sc.shape[0]!=0:    
        st.info('No Tables under the schema', icon="ℹ️")
    else:
      st.error('Please select a schema', icon="🚨") 

      
      
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
####number of tags in each table####  
    if sc_tb.shape[0]!=0 and alltags.shape[0]!=0: 
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
  

        
####col2---masking policy options####  
with tab1:
  with col2:
    st.write("masking policy options")
    c2tab1,c2tab2 = st.tabs(["Create & Apply Mask","Edit Mask"])
    with c2tab1:
      if sc_tb.shape[0]!=0 and alltags.shape[0]!=0: 
        mschema = st.selectbox('Select schema:',list(set(final['SCHEMA'])))
        mtable = st.selectbox('Select table:',list(set(final.loc[final['SCHEMA']==mschema]['TABLE NAME'])))
        final2 = final.loc[final['SCHEMA']==mschema]
        mcol = st.selectbox('Select Column:',list(set(final2.loc[final2['TABLE NAME']==mtable]['COLUMN NAME'])))
        final3 = final2.loc[final2['TABLE NAME']==mtable]
        final4dt = final3.loc[final3['COLUMN NAME']==mcol]['DATA TYPE']
        name = st.text_input('Name of the mask:')
        roles_acc = pd.read_sql("select name from SNOWFLAKE.ACCOUNT_USAGE.ROLES where deleted_on is null;",conn)
        R = []
        for i, row in roles_acc.iterrows():
          R.append(row['NAME'])
        roles = st.multiselect('Choose Roles that can see the data:',R)
        sroles = (str(roles)[1:-1])
        mdatatype = st.radio('Choose Datatype:',['String','Number'])
        if (mdatatype=='String' and str(final4dt).split()[1]=='TEXT') or (mdatatype =='Number' and str(final4dt).split()[1]=='NUMBER'):
          if st.button('Create and Apply Mask'):
            cur.execute("Use database {};".format(DB))
            cur.execute("Use Schema {};".format(mschema))
            cur.execute("Create masking policy {} as (val {}) returns {} -> case when current_role() in ({}) then val else '*********' end;".format(name,mdatatype,mdatatype,sroles))
            cur.execute("alter table {}.{}.{} modify column {} set masking policy {};".format(DB,mschema,mtable,mcol,name))        
        else:
          st.error('Data type doesnt match with the column', icon="🚨")           
      with c2tab2:
        if sc_tb.shape[0]!=0 and alltags.shape[0]!=0:
          allpolicy_tab = pd.DataFrame(columns=['DATABASE','SCHEMA', 'TABLE_NAME', 'COLUMN_NAME','POLICY_NAME'])
          for i,row in sc_tb.iterrows():
            cur.execute("Use Schema {};".format(row['SCHEMA']))
            policy_tab = pd.read_sql("select policy_db as database,policy_schema as schema,ref_entity_name as table_name,ref_column_name as column_name,\
            policy_name from table({}.information_schema.policy_references(ref_entity_name=>'{}',ref_entity_domain=>'TABLE'));".format(DB,row['TABLE_NAME']),conn)
            allpolicy_tab = allpolicy_tab.append(policy_tab,ignore_index=True) 
          pschema = st.selectbox('Choose Schema:',list(set(allpolicy_tab['SCHEMA']))) 
          sch_poli = allpolicy_tab.loc[allpolicy_tab['SCHEMA']==pschema]
          policy = st.selectbox('Choose Masking Policy:',list(set(sch_poli['POLICY_NAME'])))
          ed = st.radio('',['Edit Mask','Drop Mask'])
          if ed=='Edit Mask':
            pass
          else:
            pass
            #cur.execute("alter table {}.{}.{} modify column {} unset masking policy;".format(DB.pschema,)
  with col1:
    if sc_tb.shape[0]!=0 and alltags.shape[0]!=0:
      if allpolicy_tab.shape[0]!=0:
        st.write("Masked Columns")
        allpolicy_tab
      else:
        st.info('No Columns are Masked', icon="ℹ️")
        
        
        
        
        
        
        

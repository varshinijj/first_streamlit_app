import streamlit as st
st.title("snowflake data app")

import pandas as pd
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake22!',
                account='cy06007.ap-southeast-1',
                warehouse='MAX',
                database='SNOWFLAKE',
                schema='ACCOUNT_USAGE',
    ocsp_fail_open=False
                )
dbs = pd.read_sql("select database_name from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where deleted is NULL and database_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn)

option = st.multiselect("Pick some fruits:", dbs)
sc= pd.read_sql("select schema_name,catalog_name from SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA where deleted is NULL and catalog_name not in ('SNOWFLAKE','SNOWFLAKE_SAMPLE_DATA');",conn) 
sctab = sc.loc[option]      
st.dataframe(sc)

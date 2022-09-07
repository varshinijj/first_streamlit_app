import streamlit as st
st.title("sample")

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
dbs = pd.read_sql("show databases;",conn)
option =st.selectbox('select market segment:', dbs)

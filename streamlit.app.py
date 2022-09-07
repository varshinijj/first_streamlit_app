import streamlit as st
st.title("sample")
pip install snowflake-connector-python
import pandas as pd
import snowflake.connector
conn = snowflake.connector.connect(
                user='VARSHINI',
                password='Snowflake22!',
                account='cy06007.ap-southeast-1',
                warehouse='MAX',
                database='SNOWFLAKE_SAMPLE_DATA',
                schema='TPCH_SF1',
    ocsp_fail_open=False
                )
mktsegment = pd.read_sql("select distinct C_MKTSEGMENT from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER limit 10;",conn)
option =st.selectbox('select market segment:', mktsegment)

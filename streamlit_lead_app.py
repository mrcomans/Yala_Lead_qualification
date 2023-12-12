import streamlit as st
import pandas
import requests
import snowflake.connector
from urllib.error import URLError

st.title('Hello there let us score some leads')

# Function to connect to Snowflake
def connect_to_snowflake():
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
    return my_cnx
# my_cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")

conn = connect_to_snowflake()
my_cur = conn.cursor()
my_cur.execute("SELECT * FROM YALA_DB.PUBLIC.CONVERTEDONLY LIMIT 10")
my_data_rows = my_cur.fetchall()
st.dataframe(my_data_rows)

st.title('Connected!')


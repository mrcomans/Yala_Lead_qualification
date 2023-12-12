import streamlit
import pandas
import requests
import snowflake.connector
from urllib.error import URLError

streamlit.title('Hello there let us score some leads')

my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
my_cur = my_cnx.cursor()

my_cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")
my_cur.execute("SELECT * FROM YALA_DB.PUBLIC.CONVERTEDONLY LIMIT 10")
my_data_rows = my_cur.fetchall()
streamlit.dataframe(my_data_rows)

streamlit.title('Connected!')

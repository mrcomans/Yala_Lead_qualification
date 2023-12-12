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

# Function to process form data to match model input format
def process_input_data(form_data):
    # Process the form_data to match the model input format
    # This is where you transform the data as per your Jupyter Notebook logic
    return processed_data

# Function to score the lead
def score_lead(model, data):
    # Use the model to score the data
    return score

# Streamlit app
def main():
    st.title("Lead Scoring App")
    conn = connect_to_snowflake()
    my_cur = conn.cursor()
    my_cur.execute("SELECT * FROM YALA_DB.PUBLIC.CONVERTEDONLY LIMIT 10")
    my_data_rows = my_cur.fetchall()
    st.dataframe(my_data_rows)

main()
st.title('Connected with seperate main!')


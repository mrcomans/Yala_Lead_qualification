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
    
     # Webform creation
    created_date = st.date_input("Created Date")
    lead_description = st.text_area("Lead Description")
    lead_source_id = st.selectbox("Lead Source ID", [3367610, 2973396, 3314817, 2973397, 2985685, 3306719, 3377551, 3377552, 3377553, 3306720, 3418055, 3418056, 2985683, 2973398, 3056470, 3306721])  # Add all options
    # Add other form fields...
       
    my_cur = conn.cursor()
    my_cur.execute("SELECT * FROM YALA_DB.PUBLIC.CONVERTEDONLY LIMIT 10")
    my_data_rows = my_cur.fetchall()
    st.dataframe(my_data_rows)

if __name__ == "__main__":
    main()
    
st.title('Connected with seperate main!')


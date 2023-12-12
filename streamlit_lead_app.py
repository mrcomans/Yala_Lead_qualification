import streamlit as st
import pandas
import requests
import snowflake.connector
from urllib.error import URLError
from datetime import time

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
    # Time Input
    selected_time = st.time_input("Created Time", time(8, 45))  # Default time is 08:45
    st.write("Selected Time:", selected_time)

    lead_description = st.text_area("Lead Description")
    lead_source_id = st.selectbox("Lead Source ID", [3367610, 2973396, 3314817, 2973397, 2985685, 3306719, 3377551, 3377552, 3377553, 3306720, 3418055, 3418056, 2985683, 2973398, 3056470, 3306721])  # Add all options
    # Add other form fields...
    
    # Multiselect for Tent Types
    tent_types = ["SUPERNOVA", "DREAMER", "AURORA", "AURORA VENUE", "COMET", "ECLIPSE", "GLAMPING LODGE", "SHIMMER", "SPARKLE", "STARDUST", "SUNSHINE", "TWILIGHT", "BELL TENT", "STELLA", "VENUE STRUCTURES"]
    selected_tents = st.multiselect("Select Tent Types", tent_types, default=["DREAMER", "ECLIPSE"])
    st.write("You selected:", selected_tents)
    
    # Selectbox for Countries
    countries = ["GERMANY", "NETHERLANDS", "ITALY","SPAIN","UNITED KINGDOM","UNITED STATES", "ROMANIA", "FRANCE", "CROATIA", "DENMARK", "SOUTH AFRICA", "GAMBIA", "MAURITIUS", "UGANDA", "JAPAN", "SWITZERLAND", "VIETNAM"
    "SAUDI ARABIA", "MOROCCO", "MONTENEGRO", "SLOVENIA", "UNITED ARAB EMIRATES", "SERBIA", "BELGIUM", "QATAR", "GREECE", "EGYPT", "HUNGARY", "PHILIPPINES", "LEBANON", "SRI LANKA"
    "CZECH REPUBLIC", "OMAN", "MACEDONIA", "MALDIVES", "UKRAINE", "GEORGIA", "ARMENIA", "JORDAN", "BANGLADESH", "IRAN", "GUADELOUPE", "NETHERLANDS ANTILLES", "BOSNIA AND HERZEGOWINA"
    "ESTONIA", "PORTUGAL", "SINGAPORE", "HONG KONG", "BRAZIL", "INDIA", "CYPRUS", "BULGARIA", "PAKISTAN", "INDONESIA", "SOUTH KOREA", "NEW ZEALAND", "THAILAND", "ISRAEL"
    "SWEDEN", "MOLDOVA", "PERU", "KUWAIT", "AUSTRIA", "MALAYSIA", "REUNION", "LUXEMBOURG"]
    selected_country = st.selectbox("Country", countries)
    st.write("You selected:", selected_country)
    
    my_cur = conn.cursor()
    my_cur.execute("SELECT * FROM YALA_DB.PUBLIC.CONVERTEDONLY LIMIT 10")
    my_data_rows = my_cur.fetchall()
    st.dataframe(my_data_rows)

if __name__ == "__main__":
    main()
    
# st.title('Connected with seperate main!')


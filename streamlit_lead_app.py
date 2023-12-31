import streamlit as st
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError
import datetime
import re
import joblib
import numpy
from snowflake.ml.registry import model_registry
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.ml._internal.utils import identifier
import json

st.title('Lead Scoring App')
model = None

def create_snowpark_session():
    # Assuming your st.secrets["snowflake"] has all the necessary parameters
    session = Session.builder.configs(st.secrets["snowflake"]).create()
    # return session
    return session

# def load_model_from_file():
    # Path to your .pkl file
    # model_file_path = r'C:\Users\markt\OneDrive\Git\Yala_Lead_qualification\leads_modelv1.pkl'

    # Load the model from the file
    # model = joblib.load('leads_modelv1.pkl')
    # return model

def load_model_from_registry(model, version):
    # Create a Snowpark session
    session = create_snowpark_session()

    # Get current database and schema
    db = identifier._get_unescaped_name(session.get_current_database())
    schema = identifier._get_unescaped_name(session.get_current_schema())

    # Create a registry object
    registry = model_registry.ModelRegistry(session=session, database_name=db, schema_name=schema, create_if_not_exists=True)

    # Load the model v3
    model = registry.load_model(model, version)

    # debug information
    # st.write("model:", model)
    
    # return model
    return model

# Function to check if valid email address
def is_valid_email(email):
    """Check if the input is a valid email address."""
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.match(pattern, email) is not None

# Function to connect to Snowflake
def connect_to_snowflake():
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
    return my_cnx
# my_cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION()")

# Function to process selected created date
def process_selected_created_date(selected_created_date):
    # Assuming the format is "datetime.date(YYYY, MM, DD)"
    # Extract the date components
    # date_parts = selected_date_str.strip("datetime.date()").split(',')
    # year, month, day = map(int, date_parts)
    # Create a datetime object
    # date_obj = datetime.date(year, month, day)

    # Extract year, month, week number, and day
    pr_createdyear = selected_created_date.year
    pr_createdmonth = selected_created_date.month
    pr_createdweek = selected_created_date.isocalendar()[1]
    pr_createdday = selected_created_date.isoweekday()

    return pr_createdyear, pr_createdmonth, pr_createdweek, pr_createdday

def process_selected_time(selected_time):
    
    # Extract hour
    pr_hour = selected_time.hour
    
    return pr_hour

def process_selected_lead_description(selected_lead_description):

    # Extract normalized lead description
    pr_selected_lead_description_norm = len(selected_lead_description) / 3100
    
    return float(round(pr_selected_lead_description_norm, 4))

def process_selected_leadsourceid(selected_leadsourceid):
    
    return selected_leadsourceid

def process_selected_country(selected_country):
    
    # Convert to uppercase and replace spaces with underscores
    pr_selected_country = selected_country.upper().replace(" ", "_")
    
    return pr_selected_country

def process_selected_event(selected_event):
    
    return selected_event

def process_selected_gender(selected_gender):
    
    return selected_gender

def process_selected_mail(selected_mail):
    # List of identified private email domains
    private_domains = [
        'gmail.com', 'hotmail.com', 'orange.fr', 'wanadoo.fr', 'hotmail.fr',
        'yahoo.com', 'outlook.com', 'hotmail.co.uk', 'me.com', 'gmx.de',
        'icloud.com', 'yahoo.it', 'libero.it', 'web.de', 'kpnmail.nl',
        'yahoo.de', 'yahoo.fr', 'free.fr', 'telenet.be', 'live.fr',
        'otenet.gr', 'mac.com', 'yahoo.co.uk', 'laposte.net', 'uol.com.br',
        'casema.nl', 'aol.com', 't-online.de', 'unknown.com', 'yahoo.in',
        'gmx.fr', 'mail.com', 'mail.ru', 'live.it', 'msn.com',
        'yahoo.com.sg', 'hotmail.it', 'googlemail.com', 'hotmail.nl', 'ziggo.nl'
    ]

    # Extract the domain from the email
    domain = selected_mail.split('@')[-1]

    # Determine email type
    email_type = "PRIVATE" if domain in private_domains else "BUSINESS"
    
    return email_type

def process_selected_tents(selected_tent):
    
    return selected_tent

# Function to process form data to match model input format
def process_input_data(template_data_df, submitted_values_df):
    # Process the form_data to match the model input format
    # This is where you transform the data as per your Jupyter Notebook logic
    # processed_data_df = template_data_df
    processed_data = template_data_df.copy()

##### Extract SELECTEDCREATEDDATE value
    selected_created_date = submitted_values_df.loc[0, 'SELECTEDCREATEDDATE']

    # Process the SELECTEDCREATEDDATE
    pr_year, pr_month, pr_week, pr_day = process_selected_created_date(selected_created_date)

    # Update the processed_data DataFrame
    # processed_data.loc[0, 'CREATEDYEAR'] = pr_year
    processed_data.loc[0, 'CREATEDMONTH'] = pr_month
    processed_data.loc[0, 'CREATEDWEEK'] = pr_week
    processed_data.loc[0, 'CREATEDDAY'] = pr_day
    # st.write('Month', pr_month)
    # st.write('Week', pr_week)
    # st.write('Day', pr_day)

##### Extract SELECTEDTIME value
    selected_time = submitted_values_df.loc[0, 'SELECTEDTIME']

    # Process the SELECTEDTIME
    pr_hour = process_selected_time(selected_time)

    # Update the processed_data DataFrame
    processed_data.loc[0, 'CREATEDHOUR'] = pr_hour 
    # st.write('Hour', pr_hour)
    
##### Extract SELECTEDLEADDESCRIPTION value
    selected_lead_description = submitted_values_df.loc[0, 'SELECTEDLEADDESCRIPTION']

    # Process the SELECTEDLEADDESCRIPTION
    pr_selected_lead_description_norm = process_selected_lead_description(selected_lead_description)

    # Update the processed_data DataFrame
    processed_data.loc[0, 'LEN_LEADDESC_NORM'] = pr_selected_lead_description_norm 
    # st.write('Leaddescription norm', pr_selected_lead_description_norm)
           
##### Extract SELECTEDLEADSOURCEID value
    selected_leadsourceid = submitted_values_df.loc[0, 'SELECTEDLEADSOURCEID']

    # Process the SELECTEDLEADSOURCEID
    pr_selected_leadsourceid = process_selected_leadsourceid(selected_leadsourceid)

    # Update the processed_data DataFrame
    processed_data.loc[0, 'LS_'+ str(pr_selected_leadsourceid)] = 1.0 
    # st.write('leadsourceid', pr_selected_leadsourceid)

##### TODO Extract SELECTEDTENTS value FINAL
    selected_tents = submitted_values_df.loc[0, 'SELECTEDTENTS']

    # Assuming process_selected_tents function processes each tent name
    # and returns a processed string (e.g., 'SPARKLE' -> 'SPARKLE_PROCESSED')

    for tent in selected_tents:
        # Process each selected tent
        pr_selected_tent = process_selected_tents(tent)

        # Update the processed_data DataFrame
        processed_data.loc[0, 'TENT_TYPE_ENCODED_' + str(pr_selected_tent)] = 1.0

        # Write the processed tent to Streamlit output
        # st.write('tent', pr_selected_tent)

    # selected_tents = submitted_values_df.loc[0, 'SELECTEDTENTS']

    # For each statement to iterate through the selected_tents array?
    # Process the SELECTEDTENTS
    # pr_selected_tent = process_selected_tents(selected_tents)

    # Update the processed_data DataFrame
    # processed_data.loc[0, 'TENT_TYPE_ENCODED_'+ str(pr_selected_tent)] = 1.0
    # cycle through till te end of the array? 
    # st.write('tents', pr_selected_tent)

##### Extract SELECTEDCOUNTRY value
    selected_country = submitted_values_df.loc[0, 'SELECTEDCOUNTRY']

    # Process the SELECTEDCOUNTRY
    pr_selected_country = process_selected_country(selected_country)

    # Update the processed_data DataFrame
    processed_data.loc[0, 'AD_'+ pr_selected_country] = 1.0 
    # st.write('country', pr_selected_country)
    
##### Extract SELECTEDEVENT value
    selected_event = submitted_values_df.loc[0, 'SELECTEDEVENT']

    # Process the SELECTEDEVENT
    pr_selected_event = process_selected_event(selected_event)

    # Update the processed_data DataFrame
    processed_data.loc[0, 'EVENT_ENCODED_'+ pr_selected_event] = 1.0 
    # st.write('event', pr_selected_event)

##### Extract SELECTEDGENDER value
    selected_gender = submitted_values_df.loc[0, 'SELECTEDGENDER']

    # Process the SELECTEDGENDER
    pr_selected_gender = process_selected_gender(selected_gender)

    # Update the processed_data DataFrame
    processed_data.loc[0, 'GENDER_ENCODED_'+ pr_selected_gender] = 1.0 
    # st.write('gender', pr_selected_gender)

##### Extract SELECTEDMAIL value
    selected_mail = submitted_values_df.loc[0, 'SELECTEDMAIL']

    # Process the SELECTEDMAIL
    pr_selected_mail = process_selected_mail(selected_mail)
    # st.write('mail', pr_selected_mail)
    
    # Update the processed_data DataFrame
    processed_data.loc[0, 'ET_' + pr_selected_mail] = 1.0 
        
    # st.write(processed_data)
        
    return processed_data

# Function to score the lead
def score_lead(model, data):
    # Use the model to score the data 
    probabilities = model.predict_proba(data)

    # If you want to add a custom prefix to the output columns, you might need to handle it manually
    # since predict_proba usually returns a numpy array, not a DataFrame
    # For example:
    df_probabilities = pd.DataFrame(probabilities, columns=['PREDICT_PROBA_0', 'PREDICT_PROBA_1'])
    
    st.write("prediction", df_probabilities)
    # st.write("Lead prediction", model.predict(data))
    # model = model.to_xgboost()
    # return score
    return True

# Streamlit app
def main():
    global model
    st.header('Hello there!, let us score some leads')
    conn = connect_to_snowflake()

    # Webform creation
    selected_created_date = st.date_input("Created Date")
    # Time Input
    selected_time = st.time_input("Created Time", datetime.time(8, 45))  # Default time is 08:45
    st.write("Selected Time:", selected_time)

    selected_lead_description = st.text_area("Lead Description")
    selected_lead_source_id = st.selectbox("Lead Source ID", [3367610, 2973396, 3314817, 2973397, 2985685, 3306719, 3377551, 3377552, 3377553, 3306720, 3418055, 3418056, 2985683, 2973398, 3056470, 3306721])  # Add all options
    # Add other form fields...
    
    # Multiselect for Tent Types
    tent_types = ["SUPERNOVA", "DREAMER", "AURORA", "AURORA VENUE", "COMET", "ECLIPSE", "GLAMPING LODGE", "SHIMMER", "SPARKLE", "STARDUST", "SUNSHINE", "TWILIGHT", "BELL TENT", "STELLA", "VENUE STRUCTURES"]
    selected_tents = st.multiselect("Select Tent Types", tent_types, default=["DREAMER", "ECLIPSE"])
    # st.write("You selected:", selected_tents)
    
    # Selectbox for Countries
    countries = ["GERMANY", "NETHERLANDS", "ITALY","SPAIN","UNITED KINGDOM","UNITED STATES", "ROMANIA", "FRANCE", "CROATIA", "DENMARK", "SOUTH AFRICA", "GAMBIA", "MAURITIUS", "UGANDA", "JAPAN", "SWITZERLAND", "VIETNAM"
    "SAUDI ARABIA", "MOROCCO", "MONTENEGRO", "SLOVENIA", "UNITED ARAB EMIRATES", "SERBIA", "BELGIUM", "QATAR", "GREECE", "EGYPT", "HUNGARY", "PHILIPPINES", "LEBANON", "SRI LANKA"
    "CZECH REPUBLIC", "OMAN", "MACEDONIA", "MALDIVES", "UKRAINE", "GEORGIA", "ARMENIA", "JORDAN", "BANGLADESH", "IRAN", "GUADELOUPE", "NETHERLANDS ANTILLES", "BOSNIA AND HERZEGOWINA"
    "ESTONIA", "PORTUGAL", "SINGAPORE", "HONG KONG", "BRAZIL", "INDIA", "CYPRUS", "BULGARIA", "PAKISTAN", "INDONESIA", "SOUTH KOREA", "NEW ZEALAND", "THAILAND", "ISRAEL"
    "SWEDEN", "MOLDOVA", "PERU", "KUWAIT", "AUSTRIA", "MALAYSIA", "REUNION", "LUXEMBOURG"]
    selected_country = st.selectbox("Country", countries)
    st.write("You selected:", selected_country)

    # Selectbox for Events
    events = ["ATLANTICA_LAROCHELLE", "ATLANTICA_NIORT", "MESSE_KALKAR", "RECREATIEVAKBEURS_HARDENBERG", "SETT_MONTPELLIER", 
    "SIPAC_PADOVA", "SUN_ITALY", "THE_LEISURE_SHOW_DUBAI"]
    selected_event = st.selectbox("Event", events)
    st.write("You selected:", selected_event)

    # Gender selection using radio buttons
    selected_gender = st.radio("Gender", ["MALE", "FEMALE", "OTHER"])
    st.write("You selected:", selected_gender)
    
    # Text input for email
    selected_email = st.text_input("Email Address")

    # Validate email
    if selected_email:  # Check if email is not empty
        if is_valid_email(selected_email):
            st.success("Valid Email Address")
        else:
            st.error("Invalid Email Address")

    # Submit button
    if st.button("Score lead"):
        if selected_email and is_valid_email(selected_email):
            # load model from registry
            # model = load_model_from_registry()
            # Define model name and version
            model_name = "leads_model"
            model_version = "1"
            if model is None:
                model = load_model_from_registry(model_name, model_version)

            # Collect all form data into a dictionary
            # Convert JSON to pandas DataFrame
            template_data_df = pd.DataFrame.from_dict(data)
            # Gather the form data
            # form field values (): 
            #   selected_created_date, selected_time, selected_lead_description, selected_lead_source_id, 
            #   selected_tents, selected_country, selected_event, selected_gender, selected_email
            form_data = {
                "SELECTEDCREATEDDATE": [selected_created_date],
                "SELECTEDTIME": [selected_time],
                "SELECTEDLEADDESCRIPTION": [selected_lead_description],
                "SELECTEDLEADSOURCEID": [selected_lead_source_id],
                "SELECTEDTENTS": [selected_tents],
                "SELECTEDCOUNTRY": [selected_country],
                "SELECTEDEVENT": [selected_event],
                "SELECTEDGENDER": [selected_gender],
                "SELECTEDMAIL": [selected_email]
            }
            submitted_values_df = pd.DataFrame.from_dict(form_data)            
            # selected_tents_dict = json.loads(selected_tents)
            score_lead(model, process_input_data(template_data_df, submitted_values_df))
        else:
            st.error("Please enter a valid email address.")
        
    # my_cur = conn.cursor()
    # my_cur.execute("SELECT * FROM YALA_DB.PUBLIC.CONVERTEDONLY LIMIT 10")
    # my_data_rows = my_cur.fetchall()
    # st.dataframe(my_data_rows)

   
data = {
"CREATEDMONTH": [0.0],
"CREATEDWEEK": [0.0],
"CREATEDHOUR": [0.0],
"CREATEDDAY": [0.0],
"LEN_LEADDESC_NORM": [0.0],
"LS_2973396": [0.0],
"LS_2973397": [0.0],
"LS_2973398": [0.0],
"LS_2985683": [0.0],
"LS_2985685": [0.0],
"LS_3306719": [0.0],
"LS_3306720": [0.0],
"LS_3314817": [0.0],
"LS_3367610": [0.0],
"AD_ARMENIA": [0.0],
"AD_AUSTRIA": [0.0],
"AD_BANGLADESH": [0.0],
"AD_BELGIUM": [0.0],
"AD_BOSNIA_AND_HERZEGOWINA": [0.0],
"AD_BRAZIL": [0.0],
"AD_BULGARIA": [0.0],
"AD_CROATIA": [0.0],
"AD_CYPRUS": [0.0],
"AD_CZECH_REPUBLIC": [0.0],
"AD_DENMARK": [0.0],
"AD_EGYPT": [0.0],
"AD_ESTONIA": [0.0],
"AD_FRANCE": [0.0],
"AD_GAMBIA": [0.0],
"AD_GEORGIA": [0.0],
"AD_GERMANY": [0.0],
"AD_GREECE": [0.0],
"AD_GUADELOUPE": [0.0],
"AD_HONG_KONG": [0.0],
"AD_HUNGARY": [0.0],
"AD_INDIA": [0.0],
"AD_INDONESIA": [0.0],
"AD_IRAN": [0.0],
"AD_ISRAEL": [0.0],
"AD_ITALY": [0.0],
"AD_JAPAN": [0.0],
"AD_JORDAN": [0.0],
"AD_KUWAIT": [0.0],
"AD_LEBANON": [0.0],
"AD_LUXEMBOURG": [0.0],
"AD_MACEDONIA": [0.0],
"AD_MALAYSIA": [0.0],
"AD_MALDIVES": [0.0],
"AD_MAURITIUS": [0.0],
"AD_MOLDOVA": [0.0],
"AD_MONTENEGRO": [0.0],
"AD_MOROCCO": [0.0],
"AD_NETHERLANDS": [0.0],
"AD_NETHERLANDS_ANTILLES": [0.0],
"AD_NEW_ZEALAND": [0.0],
"AD_OMAN": [0.0],
"AD_PAKISTAN": [0.0],
"AD_PERU": [0.0],
"AD_PHILIPPINES": [0.0],
"AD_PORTUGAL": [0.0],
"AD_QATAR": [0.0],
"AD_REUNION": [0.0],
"AD_ROMANIA": [0.0],
"AD_SAUDI_ARABIA": [0.0],
"AD_SERBIA": [0.0],
"AD_SINGAPORE": [0.0],
"AD_SLOVENIA": [0.0],
"AD_SOUTH_AFRICA": [0.0],
"AD_SOUTH_KOREA": [0.0],
"AD_SPAIN": [0.0],
"AD_SRI_LANKA": [0.0],
"AD_SWEDEN": [0.0],
"AD_SWITZERLAND": [0.0],
"AD_THAILAND": [0.0],
"AD_UGANDA": [0.0],
"AD_UKRAINE": [0.0],
"AD_UNITED_ARAB_EMIRATES": [0.0],
"AD_UNITED_KINGDOM": [0.0],
"AD_UNITED_STATES": [0.0],
"AD_VIETNAM": [0.0],
"ET_BUSINESS": [0.0],
"ET_PRIVATE": [0.0],
"TENT_TYPE_ENCODED_AURORA": [0.0],
"TENT_TYPE_ENCODED_AURORA_VENUE": [0.0],
"TENT_TYPE_ENCODED_BELL_TENT": [0.0],
"TENT_TYPE_ENCODED_COMET": [0.0],
"TENT_TYPE_ENCODED_DREAMER": [0.0],
"TENT_TYPE_ENCODED_ECLIPSE": [0.0],
"TENT_TYPE_ENCODED_GLAMPING_LODGE": [0.0],
"TENT_TYPE_ENCODED_SHIMMER": [0.0],
"TENT_TYPE_ENCODED_SPARKLE": [0.0],
"TENT_TYPE_ENCODED_STARDUST": [0.0],
"TENT_TYPE_ENCODED_STELLA": [0.0],
"TENT_TYPE_ENCODED_SUNSHINE": [0.0],
"TENT_TYPE_ENCODED_SUPERNOVA": [0.0],
"TENT_TYPE_ENCODED_TWILIGHT": [0.0],
"TENT_TYPE_ENCODED_VENUE_STRUCTURES": [0.0],
"EVENT_ENCODED_ATLANTICA_LAROCHELLE": [0.0],
"EVENT_ENCODED_ATLANTICA_NIORT": [0.0],
"EVENT_ENCODED_MESSE_KALKAR": [0.0],
"EVENT_ENCODED_RECREATIEVAKBEURS_HARDENBERG": [0.0],
"EVENT_ENCODED_SETT_MONTPELLIER": [0.0],
"EVENT_ENCODED_SIPAC_PADOVA": [0.0],
"EVENT_ENCODED_SUN_ITALY": [0.0],
"EVENT_ENCODED_THE_LEISURE_SHOW_DUBAI": [0.0],
"GENDER_ENCODED_FEMALE": [0.0],
"GENDER_ENCODED_MALE": [0.0],
"GENDER_ENCODED_OTHER": [0.0]
}

# Create the DataFrame
# lead_instance_df = pd.DataFrame(data)

if __name__ == "__main__":
    main()

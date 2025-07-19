
import streamlit as st
import http.client
import json

st.title("LinkedIn JSON Scraper")

user_input = st.text_input("Enter LinkedIn Vanity URL (e.g., vidhant-jain)")

if st.button("Submit"):
    try:
        profile_slug = user_input.strip()
        conn = http.client.HTTPSConnection("linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com")

        headers = {
            'x-rapidapi-key': "e6274d5593msh7a4e71bb2fe2f44p1552bejsn76fc9770b557",  # Replace with your real key
            'x-rapidapi-host': "linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com"
        }

        conn.request("GET", f"/profile/{profile_slug}", headers=headers)
        res = conn.getresponse()
        data = res.read()
        parsed = json.loads(data.decode("utf-8"))

        st.subheader("Fetched JSON Data:")
        st.json(parsed)

    except Exception as e:
        st.error(f"Error fetching data: {e}")

import os
import json
import http.client
import streamlit as st

st.title("LinkedIn JSON Scraper")

# Put your keys in Streamlit secrets, env vars, or another secure store
API_KEYS = [
    os.getenv("RAPIDAPI_PRIMARY_KEY") or "e6274d5593msh7a4e71bb2fe2f44p1552bejsn76fc9770b557",
    os.getenv("RAPIDAPI_BACKUP_KEY")  or "a4612aa3e6msh590f437373acc6bp1bd8a0jsn9400b5615fbf",
]
RAPID_HOST = "linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com"

def fetch_profile(slug: str, api_key: str):
    conn = http.client.HTTPSConnection(RAPID_HOST, timeout=15)
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": RAPID_HOST,
    }
    conn.request("GET", f"/profile/detail?username={slug}", headers=headers)
    res = conn.getresponse()
    body = res.read()
    return res.status, body

user_input = st.text_input("Enter LinkedIn vanity URL (e.g., vidhant-jain)")

if st.button("Submit") and user_input.strip():
    profile_slug = user_input.strip()
    for idx, key in enumerate(API_KEYS, start=1):
        status, raw = fetch_profile(profile_slug, key)
        if status == 200:
            parsed = json.loads(raw.decode("utf-8"))
            st.subheader(f"Fetched JSON — API key {idx}")
            st.json(parsed)
            break
        elif status in (429, 403):
            st.warning(f"Primary key quota exhausted (status {status}), switching to backup…")
            continue
        else:
            st.error(f"API responded with status {status}: {raw.decode('utf-8')}")
            break
    else:
        st.error("All keys exhausted or invalid response. Try again later.")

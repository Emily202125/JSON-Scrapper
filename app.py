import os
import json
import http.client
from urllib.parse import quote_plus
import streamlit as st

st.title("LinkedIn JSON Scraper")

# Top-of-page status placeholder (shows which key succeeded)
top_msg = st.empty()

HOST = "linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com"
PATH = "/profile/detail?username={slug}"

def _val(name, default):
    return os.getenv(name) or default

API_KEYS = [
    _val("RAPIDAPI_PRIMARY_KEY", "e6274d5593msh7a4e71bb2fe2f44p1552bejsn76fc9770b557"),
    _val("RAPIDAPI_BACKUP_KEY",  "a4612aa3e6msh590f437373acc6bp1bd8a0jsn9400b5615fbf"),
    _val("RAPIDAPI_KEY_3",       "ed6777f53fmsh87af770e58ea6f6p185666jsn64e983e7cb23"),
    _val("RAPIDAPI_KEY_4",       "b7122951abmsh59bbe65684d72a0p1f1b76jsn95b884c46942"),
]

def ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"

def fetch_with_key(slug: str, api_key: str):
    conn = http.client.HTTPSConnection(HOST, timeout=20)
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": HOST,
    }
    try:
        conn.request("GET", PATH.format(slug=quote_plus(slug)), headers=headers)
        res = conn.getresponse()
        body = res.read()
        return res.status, body
    finally:
        conn.close()

user_input = st.text_input("Enter LinkedIn vanity handle", placeholder="vidhant-jain")

if st.button("Submit") and user_input.strip():
    slug = user_input.strip()
    top_msg.empty()  # clear previous run
    with st.spinner("Fetching..."):
        for i, key in enumerate([k for k in API_KEYS if k and k.strip()], start=1):
            status, raw = fetch_with_key(slug, key)

            if status == 200:
                # Top banner showing which key worked
                top_msg.success(f"Fetched using {ordinal(i)} API key")

                # Show body
                try:
                    parsed = json.loads(raw.decode("utf-8"))
                    st.subheader(f"Fetched JSON using key {i}")
                    st.json(parsed)
                except json.JSONDecodeError:
                    st.subheader(f"Received non JSON using key {i}")
                    st.code(raw.decode("utf-8", errors="ignore")[:2000])
                break

            if status in (429, 403):
                st.warning(f"Key {i} hit a limit or was blocked. Trying next key...")
                continue

            st.error(f"HTTP {status} with key {i}:\n{raw.decode('utf-8', errors='ignore')[:1000]}")
            break
        else:
            st.error("All keys failed or were exhausted. Try again later.")

st.caption("Tip: move keys to environment variables or st.secrets in production.")

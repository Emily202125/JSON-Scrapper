import os
import json
import time
import random
from urllib.parse import quote_plus, urlparse

import requests
import streamlit as st

st.title("LinkedIn JSON Scraper")
top_msg = st.empty()  # banner placeholder

HOST = "linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com"
BASE_URL = f"https://{HOST}/profile/detail?username={{slug}}"


def _val(name: str, default: str) -> str:
    return os.getenv(name) or default


API_KEYS = [
    _val("RAPIDAPI_PRIMARY_KEY", "e6274d5593msh7a4e71bb2fe2f44p1552bejsn76fc9770b557"),
    _val("RAPIDAPI_BACKUP_KEY", "a4612aa3e6msh590f437373acc6bp1bd8a0jsn9400b5615fbf"),
    _val("RAPIDAPI_KEY_3", "ed6777f53fmsh87af770e58ea6f6p185666jsn64e983e7cb23"),
    _val("RAPIDAPI_KEY_4", "b7122951abmsh59bbe65684d72a0p1f1b76jsn95b884c46942"),
]

CONNECT_TIMEOUT = 6
READ_TIMEOUT = 18
MAX_RETRIES_PER_KEY = 2  # total tries per key = 1 + this value

TRANSIENT_STATUSES = {408, 425, 500, 502, 503, 504}
QUOTA_STATUSES = {429, 401, 403}

HEADERS_BASE = {
    "x-rapidapi-host": HOST,
    "Accept": "application/json, text/plain;q=0.8, */*;q=0.5",
    "User-Agent": "streamlit-linkedin-json-scraper/1.0",
}


def ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


def normalize_slug(text: str) -> str:
    t = text.strip()
    if t.startswith("http"):
        try:
            p = urlparse(t)
            segs = [s for s in p.path.split("/") if s]
            if segs:
                return segs[-1].strip("/")
        except Exception:
            pass
    return t


def call_api(slug: str, api_key: str) -> requests.Response:
    headers = dict(HEADERS_BASE)
    headers["x-rapidapi-key"] = api_key
    url = BASE_URL.format(slug=quote_plus(slug))
    return requests.get(url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))


user_input = st.text_input(
    "Enter LinkedIn vanity handle or profile URL",
    placeholder="vidhant-jain or https://linkedin.com/in/vidhant-jain/",
)

if st.button("Submit") and user_input.strip():
    try:
        slug = normalize_slug(user_input)
        success = False
        logs = []

        with st.spinner("Fetching..."):
            for i, key in enumerate([k for k in API_KEYS if k and k.strip()], start=1):
                for attempt in range(1, MAX_RETRIES_PER_KEY + 2):
                    try:
                        resp = call_api(slug, key)
                        status, text = resp.status_code, resp.text

                        if status == 200:
                            top_msg.success(f"Fetched using {ordinal(i)} API key")
                            try:
                                st.subheader(f"Fetched JSON using key {i}")
                                st.json(resp.json())
                            except json.JSONDecodeError:
                                st.subheader(f"Received non JSON using key {i}")
                                st.code(text[:2000])
                            logs.append(f"Key {i} success on attempt {attempt}.")
                            success = True
                            break

                        if status in QUOTA_STATUSES:
                            st.warning(
                                f"Key {i} hit a limit or was blocked (HTTP {status}). Trying next key..."
                            )
                            logs.append(f"Key {i} quota/blocked (HTTP {status}).")
                            break  # move to next key

                        if status in TRANSIENT_STATUSES:
                            logs.append(
                                f"Key {i} transient HTTP {status} on attempt {attempt}. Retrying..."
                            )
                            time.sleep(
                                (0.6 * (2 ** (attempt - 1))) + random.uniform(0, 0.4)
                            )
                            continue

                        # Hard error
                        st.error(f"HTTP {status} with key {i}:\n{text[:1200]}")
                        logs.append(
                            f"Key {i} hard HTTP {status}. Body: {text[:200]}"
                        )
                        break  # stop trying this key

                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                        logs.append(
                            f"Key {i} network error on attempt {attempt}: {e}. Retrying..."
                        )
                        time.sleep(
                            (0.6 * (2 ** (attempt - 1))) + random.uniform(0, 0.4)
                        )
                        continue

                    except Exception as e:
                        st.error(f"Unexpected error with key {i}: {e}")
                        logs.append(f"Key {i} unexpected error: {e}")
                        break  # move to next key

                if success:
                    break  # stop after first success

        if not success:
            st.error("All keys failed or were exhausted after retries.")

        with st.expander("Debug log"):
            st.write("\n".join(logs))

    except Exception as outer:
        # Final safety net so the app never throws a raw traceback
        st.error(f"Something went wrong: {outer}")
        st.info("Check Debug log and your RapidAPI usage. The app did not crash.")


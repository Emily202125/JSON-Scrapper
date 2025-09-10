import os
import json
import time
import random
from urllib.parse import urlparse
import requests
import streamlit as st

st.title("LinkedIn JSON Scraper • Bright Data")

# ---------- Config ----------
BRIGHTDATA_API_KEY = os.getenv(
    "BRIGHTDATA_API_KEY",
    "8b81c258b672bca0a33d06f16629919e80e1facf10074179de06e476d66b2c72"  # replace in prod, or set env/Secrets
)
BRIGHTDATA_DATASET_ID = os.getenv("BRIGHTDATA_DATASET_ID", "")  # e.g. gd_l1vikfnt1wgvvqz95w
SYNC_TIMEOUT_SEC = 60  # Bright Data sync limit before 202 with snapshot_id
POLL_INTERVAL_SEC = 2
MAX_POLL_SEC = 90

SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
    "Content-Type": "application/json",
})

def normalize_profile_url(text: str) -> str:
    """
    Accepts a vanity handle like `vidhant-jain` or a full URL,
    returns a full LinkedIn profile URL.
    """
    t = (text or "").strip()
    if not t:
        return ""
    if t.startswith("http"):
        # Trust user URL if it looks like LinkedIn
        return t
    # Treat as vanity handle
    slug = t.strip("/").split("?")[0]
    return f"https://www.linkedin.com/in/{slug}/"

def scrape_sync(dataset_id: str, url: str):
    """
    Try synchronous scrape. If server needs more time, Bright Data returns 202 with snapshot_id.
    """
    endpoint = f"https://api.brightdata.com/datasets/v3/scrape"
    params = {"dataset_id": dataset_id}
    body = {"input": [{"url": url}]}
    resp = SESSION.post(endpoint, params=params, data=json.dumps(body), timeout=90)
    return resp

def trigger_async(dataset_id: str, url: str):
    """
    Trigger async job and return snapshot_id.
    """
    endpoint = f"https://api.brightdata.com/datasets/v3/trigger"
    params = {"dataset_id": dataset_id, "include_errors": "true"}
    body = [{"url": url}]
    resp = SESSION.post(endpoint, params=params, data=json.dumps(body), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("snapshot_id")

def fetch_snapshot(snapshot_id: str):
    """
    Poll snapshot until results are ready. Returns (json_data, err_text)
    """
    snap_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
    started = time.time()
    while True:
        resp = SESSION.get(snap_url, params={"format": "json"}, timeout=30)
        # 202 means not ready yet
        if resp.status_code == 202:
            if time.time() - started > MAX_POLL_SEC:
                return None, f"Timed out waiting for snapshot {snapshot_id}."
            time.sleep(POLL_INTERVAL_SEC)
            continue
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code} while getting snapshot: {resp.text[:800]}"
        try:
            return resp.json(), None
        except json.JSONDecodeError:
            return None, f"Snapshot returned non JSON: {resp.text[:800]}"

# ---------- UI ----------
with st.sidebar:
    st.subheader("Bright Data Settings")
    dataset_id_input = st.text_input(
        "Dataset ID (LinkedIn Profiles API)",
        value=BRIGHTDATA_DATASET_ID,
        placeholder="gd_xxxxxxxxxxxxxxxxx",
        help="From Bright Data dashboard for the LinkedIn Profiles API dataset."
    )
    use_sync_first = st.checkbox("Try synchronous first", value=True)
    custom_fields = st.text_input(
        "Optional: custom_output_fields",
        value="",
        placeholder="name|headline|about.updated_on",
        help="Leave blank for full payload"
    )

profile_in = st.text_input(
    "Enter LinkedIn vanity or profile URL",
    placeholder="vidhant-jain or https://www.linkedin.com/in/vidhant-jain/"
)

if st.button("Fetch"):

    if not dataset_id_input.strip():
        st.error("Please provide your Bright Data dataset_id for the LinkedIn Profiles API.")
        st.stop()

    url = normalize_profile_url(profile_in)
    if not url or "linkedin.com" not in url:
        st.error("Please enter a valid LinkedIn profile URL or vanity handle.")
        st.stop()

    st.info(f"Fetching profile: {url}")
    logs = []

    try:
        if use_sync_first:
            params = {"dataset_id": dataset_id_input}
            if custom_fields.strip():
                params["custom_output_fields"] = custom_fields.strip()

            with st.spinner("Bright Data sync scrape in progress..."):
                resp = SESSION.post(
                    "https://api.brightdata.com/datasets/v3/scrape",
                    params=params,
                    data=json.dumps({"input": [{"url": url}]}),
                    timeout=90
                )
                if resp.status_code == 200:
                    st.success("Fetched via synchronous scrape")
                    st.json(resp.json())
                    st.stop()
                elif resp.status_code == 202:
                    # Need to poll snapshot
                    data = resp.json()
                    snapshot_id = data.get("snapshot_id")
                    logs.append(f"Sync returned 202, snapshot_id={snapshot_id}, switching to polling")
                    if not snapshot_id:
                        st.error(f"Sync returned 202 without snapshot_id. Raw: {data}")
                        st.stop()
                    with st.spinner("Waiting for snapshot to complete..."):
                        result, err = fetch_snapshot(snapshot_id)
                        if err:
                            st.error(err)
                        else:
                            st.success("Fetched via async snapshot after sync timeout")
                            st.json(result)
                        st.stop()
                else:
                    logs.append(f"Sync HTTP {resp.status_code}: {resp.text[:400]}")

        # If sync off, or sync failed, do async trigger
        with st.spinner("Triggering async collection..."):
            snapshot_id = trigger_async(dataset_id_input, url)
            logs.append(f"Triggered async snapshot_id={snapshot_id}")
            result, err = fetch_snapshot(snapshot_id)
            if err:
                st.error(err)
            else:
                st.success("Fetched via async snapshot")
                st.json(result)

    except requests.exceptions.RequestException as e:
        st.error(f"Network or API error: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")

    with st.expander("Debug log"):
        st.write("\n".join(logs))


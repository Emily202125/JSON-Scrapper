import time
import json
import requests
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

st.title("LinkedIn → Bright Data (Async, Hardcoded)")

# ====== Hardcoded creds ======
BRIGHTDATA_API_KEY = "8b81c258b672bca0a33d06f16629919e80e1facf10074179de06e476d66b2c72"
DATASET_ID = "gd_l1viktl72bvl7bjuj0"
# ============================

# Networking: shorter reads + retries to avoid long hangs
SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
    "Content-Type": "application/json",
})
retry = Retry(
    total=3,
    backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
SESSION.mount("https://", HTTPAdapter(max_retries=retry))

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 20  # shorter read so we fail fast and keep polling

def normalize_url(x: str) -> str:
    x = (x or "").strip()
    if not x:
        return ""
    if x.startswith("http"):
        return x
    return f"https://www.linkedin.com/in/{x.strip('/').split('?')[0]}/"

def clean_payload(obj):
    # remove snapshot noise before showing
    SNAP_KEYS = {"snapshot_id", "snapshot_url", "snapshots", "dataset_id"}
    if isinstance(obj, dict):
        return {k: clean_payload(v) for k, v in obj.items() if k not in SNAP_KEYS}
    if isinstance(obj, list):
        return [clean_payload(x) for x in obj]
    return obj

def trigger_async(url: str):
    endpoint = "https://api.brightdata.com/datasets/v3/trigger"
    params = {"dataset_id": DATASET_ID, "include_errors": "true"}
    body = [{"url": url}]
    r = SESSION.post(
        endpoint, params=params, json=body, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Trigger error {r.status_code}: {r.text[:400]}")
    data = r.json()
    snap = data.get("snapshot_id")
    if not snap:
        raise RuntimeError(f"No snapshot_id in trigger response: {data}")
    return snap

def poll_snapshot(snapshot_id: str, max_seconds: int = 120, interval: float = 2.0):
    snap_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
    deadline = time.time() + max_seconds
    while time.time() < deadline:
        r = SESSION.get(
            snap_url, params={"format": "json"}, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        if r.status_code == 202:
            time.sleep(interval)
            continue
        if r.status_code == 200:
            return r.json()
        # for non-202/200, brief wait then retry until deadline
        time.sleep(interval)
    raise TimeoutError("Timed out waiting for Bright Data snapshot")

def scrape_profile(url: str):
    # Always use async route to avoid long blocking reads
    snapshot_id = trigger_async(url)
    data = poll_snapshot(snapshot_id, max_seconds=180, interval=2.0)
    return clean_payload(data)

profile_in = st.text_input(
    "LinkedIn handle or URL",
    placeholder="vidhant-jain or https://www.linkedin.com/in/vidhant-jain/"
)

if st.button("Fetch"):
    url = normalize_url(profile_in)
    if not url or "linkedin.com" not in url:
        st.error("Enter a valid LinkedIn profile handle or URL.")
    else:
        with st.spinner("Fetching…"):
            try:
                result = scrape_profile(url)
                st.success("Done")
                st.json(result)
            except Exception as e:
                st.error(str(e))

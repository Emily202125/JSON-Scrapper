# app.py
import os
import time
import json
import re
from typing import Any, Dict, List, Union
from urllib.parse import urlparse

import requests
import streamlit as st

# -----------------------------------
# Hardcoded defaults per your request
# -----------------------------------
DEFAULT_API_KEY = os.getenv("BRIGHTDATA_API_KEY", "8b81c258b672bca0a33d06f16629919e80e1facf10074179de06e476d66b2c72")
DEFAULT_DATASET_ID = os.getenv("BRIGHTDATA_DATASET_ID", "gd_l1viktl72bvl7bjuj0")

# If you ever want to switch to secrets, set BRIGHTDATA_API_KEY in Streamlit secrets or env.
API_KEY = st.secrets.get("BRIGHTDATA_API_KEY", DEFAULT_API_KEY)
DATASET_ID = st.secrets.get("BRIGHTDATA_DATASET_ID", DEFAULT_DATASET_ID)

BASE = "https://api.brightdata.com"
TRIGGER_URL = f"{BASE}/datasets/v3/trigger"
PROGRESS_URL = f"{BASE}/datasets/v3/progress"
SNAPSHOT_URL = f"{BASE}/datasets/v3/snapshot"

CONNECT_TIMEOUT = 3
READ_TIMEOUT = 3
TOTAL_WAIT_SECONDS = 5     # target: return within ~5s
POLL_INTERVAL = 0.7

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "streamlit-brightdata-linkedin-json/1.0",
}

# Strip images from the JSON
IMAGE_KEY_REGEX = re.compile(r"(image|photo|picture|banner|avatar|logo|background|cover)", re.I)
IMAGE_URL_REGEX = re.compile(r"\.(jpg|jpeg|png|gif|webp)$", re.I)
LIKELY_CDN_REGEX = re.compile(r"(licdn|media\.linkedin|cdn|twimg|fbcdn)", re.I)


def normalize_profile_input(text: str) -> str:
    t = text.strip()
    if not t:
        return t
    if t.startswith("http"):
        try:
            p = urlparse(t)
            segs = [s for s in p.path.split("/") if s]
            if segs:
                slug = segs[-1].strip("/")
                return f"https://www.linkedin.com/in/{slug}/"
        except Exception:
            pass
        return t
    return f"https://www.linkedin.com/in/{t.strip('/')}/"


def trigger_collection(dataset_id: str, profile_url: str) -> str:
    """
    Trigger a dataset run for the LinkedIn profile.
    We send both 'url' and 'profile_url' keys to be safe with template schemas.
    Returns snapshot_id.
    """
    payload = [{"url": profile_url, "profile_url": profile_url}]
    params = {"dataset_id": dataset_id, "include_errors": "true"}

    r = requests.post(
        TRIGGER_URL,
        headers=HEADERS,
        params=params,
        data=json.dumps(payload),
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    r.raise_for_status()
    ct = r.headers.get("content-type", "").lower()
    data = r.json() if "application/json" in ct else {}
    snap_id = data.get("snapshot_id") or data.get("id") or ""
    if not snap_id:
        raise RuntimeError(f"Could not get snapshot_id. Raw: {data}")
    return snap_id


def poll_ready(snapshot_id: str, timeout_s: float = TOTAL_WAIT_SECONDS) -> Dict[str, Any]:
    start = time.time()
    while True:
        r = requests.get(
            f"{PROGRESS_URL}/{snapshot_id}",
            headers=HEADERS,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        r.raise_for_status()
        prog = r.json() if "application/json" in r.headers.get("content-type", "").lower() else {}
        status = (prog.get("status") or prog.get("state") or "").lower()

        if status in {"done", "ready", "success", "completed"}:
            return prog
        if time.time() - start >= timeout_s:
            return prog
        time.sleep(POLL_INTERVAL)


def download_snapshot_json(snapshot_id: str) -> Union[List[Any], Dict[str, Any]]:
    params = {"format": "json", "part": "1", "batch_size": "1000"}
    r = requests.get(
        f"{SNAPSHOT_URL}/{snapshot_id}",
        headers=HEADERS,
        params=params,
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    r.raise_for_status()
    return r.json()


def prune_images(obj: Any) -> Any:
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            if IMAGE_KEY_REGEX.search(k):
                continue
            cleaned = prune_images(v)
            # drop if string looks like an image link
            if isinstance(cleaned, str) and (IMAGE_URL_REGEX.search(cleaned) or LIKELY_CDN_REGEX.search(cleaned)):
                continue
            new[k] = cleaned
        return new
    if isinstance(obj, list):
        return [prune_images(v) for v in obj]
    if isinstance(obj, str):
        if IMAGE_URL_REGEX.search(obj) or LIKELY_CDN_REGEX.search(obj):
            return ""
    return obj


# ---------------- UI ----------------
st.set_page_config(page_title="LinkedIn JSON Scraper (Bright Data)", layout="centered")
st.title("LinkedIn JSON Scraper (Bright Data) — JSON only")

with st.sidebar:
    st.markdown("**Settings**")
    dataset_id = st.text_input("Dataset ID", value=DATASET_ID)
    st.caption("API key is preloaded. You can override via secrets or env if you want.")

profile_input = st.text_input(
    "Enter LinkedIn handle or profile URL",
    placeholder="vidhant-jain or https://www.linkedin.com/in/vidhant-jain/",
)

if st.button("Fetch JSON"):
    if not API_KEY.strip():
        st.error("API key missing. Update DEFAULT_API_KEY, env, or Streamlit secrets.")
    elif not dataset_id.strip():
        st.error("Dataset ID missing.")
    elif not profile_input.strip():
        st.error("Please enter a LinkedIn handle or profile URL.")
    else:
        logs = []
        try:
            url = normalize_profile_input(profile_input)
            st.info(f"Target: {url}")

            with st.spinner("Triggering collection..."):
                snap_id = trigger_collection(dataset_id.strip(), url)
            logs.append(f"Triggered snapshot_id: {snap_id}")

            with st.spinner("Polling status (up to ~5s)..."):
                prog = poll_ready(snap_id, timeout_s=TOTAL_WAIT_SECONDS)
                status = (prog.get("status") or prog.get("state") or "").lower()
                st.write("Status:", status or "(unknown)")

            if status in {"done", "ready", "success", "completed"}:
                data = download_snapshot_json(snap_id)
                cleaned = prune_images(data)
                st.subheader("Profile JSON (images removed)")
                st.json(cleaned)
            else:
                st.warning("Snapshot not ready within 5 seconds. Try again in a few seconds.")

        except requests.HTTPError as e:
            body = ""
            try:
                body = e.response.text[:800]
            except Exception:
                pass
            st.error(f"HTTP error: {e}\n{body}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

        with st.expander("Debug log"):
            st.code("\n".join(logs) if logs else "No logs.")

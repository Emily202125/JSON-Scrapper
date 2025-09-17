# app.py
import os
import time
import json
import re
from typing import Any, Dict, List, Union
from urllib.parse import urlparse

import requests
import streamlit as st

# -----------------------------
# Hardcoded defaults (yours)
# -----------------------------
DEFAULT_API_KEY = "8b81c258b672bca0a33d06f16629919e80e1facf10074179de06e476d66b2c72"
DEFAULT_DATASET_ID = "gd_l1viktl72bvl7bjuj0"

API_KEY = st.secrets.get("BRIGHTDATA_API_KEY", os.getenv("BRIGHTDATA_API_KEY", DEFAULT_API_KEY))
DATASET_ID = st.secrets.get("BRIGHTDATA_DATASET_ID", os.getenv("BRIGHTDATA_DATASET_ID", DEFAULT_DATASET_ID))

BASE = "https://api.brightdata.com"
TRIGGER_URL = f"{BASE}/datasets/v3/trigger"
PROGRESS_URL = f"{BASE}/datasets/v3/progress"
SNAPSHOT_URL = f"{BASE}/datasets/v3/snapshot"

# Tight timeouts for responsive UI. We avoid long-hold calls.
CONNECT_TIMEOUT = 3
READ_TIMEOUT = 6
TOTAL_POLL_SECONDS = 5
POLL_INTERVAL = 0.7

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "streamlit-brightdata-linkedin-json/3.0",
}

# Remove images anywhere in the payload
IMAGE_KEY_REGEX = re.compile(r"(image|photo|picture|banner|avatar|logo|background|cover|media|images|thumb)", re.I)
IMAGE_URL_REGEX = re.compile(r"\.(jpg|jpeg|png|gif|webp|svg)$", re.I)
LIKELY_CDN_REGEX = re.compile(r"(licdn|media\.linkedin|cdn|twimg|fbcdn|akamai|cloudfront)", re.I)


def normalize_to_profile_url(text: str) -> str:
    t = (text or "").strip()
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
            return t
        return t
    return f"https://www.linkedin.com/in/{t.strip('/')}/"


def trigger_run(dataset_id: str, profile_url: str) -> str:
    """
    Start a dataset run. This LinkedIn Profiles dataset expects [{"url": "..."}].
    Returns snapshot_id on success.
    """
    payload = [{"url": profile_url}]
    params = {"dataset_id": dataset_id, "include_errors": "true"}

    # Simple retry for transient issues
    for attempt in range(1, 3):
        r = requests.post(
            TRIGGER_URL,
            headers=HEADERS,
            params=params,
            data=json.dumps(payload),
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        if r.status_code >= 500:
            time.sleep(0.6 * attempt)
            continue
        r.raise_for_status()
        data = try_json(r.text)
        snap_id = data.get("snapshot_id") or data.get("id") or ""
        if not snap_id:
            raise RuntimeError(f"trigger_run did not return snapshot_id. Raw: {r.text[:800]}")
        return snap_id

    raise RuntimeError("Failed to trigger dataset after retries.")


def poll_progress(snapshot_id: str, timeout_s: float = TOTAL_POLL_SECONDS) -> Dict[str, Any]:
    """
    Poll progress up to timeout_s. Returns the last progress JSON.
    """
    start = time.time()
    last = {}
    while True:
        r = requests.get(
            f"{PROGRESS_URL}/{snapshot_id}",
            headers=HEADERS,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
        r.raise_for_status()
        last = try_json(r.text)
        status = (last.get("status") or last.get("state") or "").lower()
        if status in {"done", "ready", "success", "completed"}:
            return last
        if time.time() - start >= timeout_s:
            return last
        time.sleep(POLL_INTERVAL)


def download_snapshot(snapshot_id: str) -> Union[List[Any], Dict[str, Any]]:
    """
    Download the snapshot as JSON. Handles JSON arrays or objects.
    """
    params = {"format": "json", "part": "1", "batch_size": "1000"}
    r = requests.get(
        f"{SNAPSHOT_URL}/{snapshot_id}",
        headers=HEADERS,
        params=params,
        timeout=(CONNECT_TIMEOUT, 10),
    )
    r.raise_for_status()
    return parse_payload(r.text)


def try_json(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        s2 = s.strip().strip('"')
        try:
            return json.loads(s2)
        except Exception:
            return {}


def parse_payload(body: str) -> Union[List[Any], Dict[str, Any]]:
    # Try direct JSON
    try:
        return json.loads(body)
    except Exception:
        pass
    # Try NDJSON
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    if len(lines) > 1:
        out = []
        all_ok = True
        for ln in lines:
            try:
                out.append(json.loads(ln))
            except Exception:
                all_ok = False
                break
        if all_ok:
            return out
    # Try double-encoded
    s2 = body.strip().strip('"')
    try:
        return json.loads(s2)
    except Exception:
        return {"raw": body[:2000]}


def prune_images(obj: Any) -> Any:
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            if IMAGE_KEY_REGEX.search(k or ""):
                continue
            cleaned = prune_images(v)
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
st.set_page_config(page_title="LinkedIn JSON - Bright Data", layout="centered")
st.title("LinkedIn JSON Scraper - Bright Data (No Images)")

# Keep last snapshot_id to avoid retriggering every click
if "last_snapshot_id" not in st.session_state:
    st.session_state.last_snapshot_id = ""

with st.sidebar:
    st.markdown("**Settings**")
    dataset_id = st.text_input("Dataset ID", value=DATASET_ID)
    st.caption("API key is preloaded. You can override using Streamlit secrets or env.")
    st.write("API key loaded:", "✅" if API_KEY.strip() else "❌")
    if st.session_state.last_snapshot_id:
        st.caption(f"Last snapshot: {st.session_state.last_snapshot_id}")

profile_input = st.text_input(
    "Enter LinkedIn handle or profile URL",
    placeholder="vidhant-jain or https://www.linkedin.com/in/vidhant-jain/",
)

col1, col2 = st.columns(2)
with col1:
    fetch_clicked = st.button("Fetch JSON")
with col2:
    check_clicked = st.button("Check status")

if fetch_clicked:
    if not API_KEY.strip():
        st.error("Missing API key.")
    elif not dataset_id.strip():
        st.error("Dataset ID is required.")
    elif not profile_input.strip():
        st.error("Please enter a LinkedIn handle or URL.")
    else:
        try:
            url = normalize_to_profile_url(profile_input)
            st.info(f"Target: {url}")

            with st.spinner("Triggering collection..."):
                snap_id = trigger_run(dataset_id.strip(), url)
                st.session_state.last_snapshot_id = snap_id
            st.success(f"Triggered. snapshot_id: {snap_id}")

            with st.spinner("Polling up to ~5s..."):
                prog = poll_progress(snap_id, timeout_s=TOTAL_POLL_SECONDS)
                status = (prog.get("status") or prog.get("state") or "").lower()
                st.write("Status:", status or "(unknown)")

            if status in {"done", "ready", "success", "completed"}:
                data = download_snapshot(snap_id)
                st.subheader("Profile JSON (images removed)")
                st.json(prune_images(data))
            else:
                st.warning("Still processing. Click 'Check status' in a few seconds.")

        except requests.HTTPError as e:
            body = ""
            try:
                body = e.response.text[:1000]
            except Exception:
                pass
            st.error(f"HTTP error: {e}\n{body}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

if check_clicked:
    if not st.session_state.last_snapshot_id:
        st.warning("No previous snapshot to check. Use Fetch JSON first.")
    else:
        try:
            snap_id = st.session_state.last_snapshot_id
            with st.spinner("Checking progress..."):
                prog = poll_progress(snap_id, timeout_s=TOTAL_POLL_SECONDS)
                status = (prog.get("status") or prog.get("state") or "").lower()
                st.write("Status:", status or "(unknown)")

            if status in {"done", "ready", "success", "completed"}:
                data = download_snapshot(snap_id)
                st.subheader("Profile JSON (images removed)")
                st.json(prune_images(data))
            else:
                st.info("Not ready yet. Check again in a bit.")

        except requests.HTTPError as e:
            body = ""
            try:
                body = e.response.text[:1000]
            except Exception:
                pass
            st.error(f"HTTP error: {e}\n{body}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

# app.py
import os
import json
import time
import re
from typing import Any, Dict, List, Union
from urllib.parse import urlparse

import requests
import streamlit as st

# -----------------------------=
# Hardcoded defaults (yours)
# -----------------------------
DEFAULT_API_KEY = "8b81c258b672bca0a33d06f16629919e80e1facf10074179de06e476d66b2c72"
DEFAULT_DATASET_ID = "gd_l1viktl72bvl7bjuj0"

# Allow overriding via Streamlit secrets or env if you ever want to
API_KEY = st.secrets.get("BRIGHTDATA_API_KEY", os.getenv("BRIGHTDATA_API_KEY", DEFAULT_API_KEY))
DATASET_ID = st.secrets.get("BRIGHTDATA_DATASET_ID", os.getenv("BRIGHTDATA_DATASET_ID", DEFAULT_DATASET_ID))

BASE = "https://api.brightdata.com"
SCRAPE_URL = f"{BASE}/datasets/v3/scrape"
PROGRESS_URL = f"{BASE}/datasets/v3/progress"
SNAPSHOT_URL = f"{BASE}/datasets/v3/snapshot"

# Tight timing so the UI responds fast
CONNECT_TIMEOUT = 4
READ_TIMEOUT = 12
TOTAL_WAIT_SECONDS = 5       # poll cap
POLL_INTERVAL = 0.7

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "streamlit-brightdata-linkedin-json/2.0",
}

# Remove image-ish fields or values anywhere in the JSON
IMAGE_KEY_REGEX = re.compile(r"(image|photo|picture|banner|avatar|logo|background|cover|media|images|thumbnails?)", re.I)
IMAGE_URL_REGEX = re.compile(r"\.(jpg|jpeg|png|gif|webp|svg)$", re.I)
LIKELY_CDN_REGEX = re.compile(r"(licdn|media\.linkedin|cdn|twimg|fbcdn|akamai|cloudfront)", re.I)


def normalize_to_profile_url(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return t
    if t.startswith("http"):
        # Convert any profile-like URL to /in/<slug>/
        try:
            p = urlparse(t)
            segs = [s for s in p.path.split("/") if s]
            if segs:
                slug = segs[-1].strip("/")
                return f"https://www.linkedin.com/in/{slug}/"
        except Exception:
            return t
        return t
    # treat as handle
    return f"https://www.linkedin.com/in/{t.strip('/')}/"


def request_sync_scrape(dataset_id: str, profile_url: str) -> requests.Response:
    """
    Try the synchronous scrape first (fastest path).
    If it times out, Bright Data returns 202 with a snapshot_id (handled later).
    """
    payload = {
        "input": [
            {"url": profile_url}  # LinkedIn Profiles "Collect by URL" expects "url"
        ]
        # You can add "custom_output_fields": "url|name|about|experience" later if desired.
    }
    params = {
        "dataset_id": dataset_id,
        "format": "json",
        "include_errors": "true",
    }
    return requests.post(
        SCRAPE_URL,
        headers=HEADERS,
        params=params,
        data=json.dumps(payload),
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )


def poll_ready(snapshot_id: str, timeout_s: float = TOTAL_WAIT_SECONDS) -> Dict[str, Any]:
    """Poll progress until ready or timeout; return last progress JSON."""
    start = time.time()
    while True:
        r = requests.get(f"{PROGRESS_URL}/{snapshot_id}", headers=HEADERS, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        r.raise_for_status()
        prog = safe_json(r.text)
        status = (prog.get("status") or prog.get("state") or "").lower()
        if status in {"done", "ready", "success", "completed"}:
            return prog
        if time.time() - start >= timeout_s:
            return prog
        time.sleep(POLL_INTERVAL)


def download_snapshot_json(snapshot_id: str) -> Union[List[Any], Dict[str, Any]]:
    """Download snapshot as JSON; tolerate lists, dicts, or NDJSON."""
    params = {"format": "json", "part": "1", "batch_size": "1000"}
    r = requests.get(f"{SNAPSHOT_URL}/{snapshot_id}", headers=HEADERS, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    r.raise_for_status()
    return robust_payload_to_json(r.text)


def safe_json(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s)
    except Exception:
        # Some responses arrive as JSON stringified twice or with stray whitespace
        s2 = s.strip().strip('"')
        try:
            return json.loads(s2)
        except Exception:
            return {}


def robust_payload_to_json(body: str) -> Union[List[Any], Dict[str, Any]]:
    """
    Handle:
      - standard JSON array/object
      - stringified JSON
      - NDJSON (one JSON per line)
    """
    # 1) direct JSON
    try:
        return json.loads(body)
    except Exception:
        pass

    # 2) NDJSON
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    if len(lines) > 1:
        out = []
        ok = False
        for ln in lines:
            try:
                out.append(json.loads(ln))
                ok = True
            except Exception:
                # If ANY line isn't JSON, bail out to fallback
                ok = False
                break
        if ok:
            return out

    # 3) double-encoded or quoted
    s2 = body.strip().strip('"')
    try:
        return json.loads(s2)
    except Exception:
        # Last resort: wrap as object so UI doesn't crash
        return {"raw": body[:2000]}


def prune_images(obj: Any) -> Any:
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            if IMAGE_KEY_REGEX.search(k or ""):
                continue
            cleaned = prune_images(v)
            # If string looks like image URL or CDN, drop it
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
st.set_page_config(page_title="LinkedIn JSON (Bright Data)", layout="centered")
st.title("LinkedIn JSON Scraper — Bright Data (No Images)")

with st.sidebar:
    st.markdown("**Credentials**")
    dataset_id = st.text_input("Dataset ID", value=DATASET_ID)
    st.caption("Uses synchronous scrape for speed. Falls back to snapshot polling if needed.")
    key_ok = bool(API_KEY.strip())
    st.write("API key loaded:", "✅" if key_ok else "❌")

profile_input = st.text_input(
    "Enter LinkedIn handle or URL",
    placeholder="vidhant-jain or https://www.linkedin.com/in/vidhant-jain/",
)

if st.button("Fetch JSON"):
    if not key_ok:
        st.error("Missing API key.")
    elif not dataset_id.strip():
        st.error("Dataset ID is required.")
    elif not profile_input.strip():
        st.error("Please enter a LinkedIn handle or profile URL.")
    else:
        logs = []
        try:
            url = normalize_to_profile_url(profile_input)
            st.info(f"Target: {url}")

            # 1) Try synchronous scrape
            with st.spinner("Scraping (sync)…"):
                resp = request_sync_scrape(dataset_id.strip(), url)

            # If sync returns 202, extract snapshot_id and poll
            if resp.status_code == 202:
                data = safe_json(resp.text)
                snap_id = data.get("snapshot_id", "")
                if not snap_id:
                    st.error(f"Timed out but no snapshot_id in response.\nRaw: {resp.text[:800]}")
                else:
                    logs.append(f"Received snapshot_id: {snap_id}")
                    with st.spinner("Polling snapshot (<= ~5s)…"):
                        prog = poll_ready(snap_id, timeout_s=TOTAL_WAIT_SECONDS)
                        status = (prog.get("status") or prog.get("state") or "").lower()
                        st.write("Status:", status or "(unknown)")
                        if status in {"done", "ready", "success", "completed"}:
                            snap_json = download_snapshot_json(snap_id)
                            st.subheader("Profile JSON (images removed)")
                            st.json(prune_images(snap_json))
                        else:
                            st.warning("Snapshot still processing. Try again in a few seconds.")
            else:
                # 200 or other
                resp.raise_for_status()
                payload = robust_payload_to_json(resp.text)
                cleaned = prune_images(payload)
                st.subheader("Profile JSON (images removed)")
                st.json(cleaned)

        except requests.HTTPError as e:
            body = ""
            try:
                body = e.response.text[:1000]
            except Exception:
                pass
            st.error(f"HTTP error: {e}\n{body}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

        with st.expander("Debug log"):
            st.code("\n".join(logs) if logs else "No logs.")

import time
import json
import requests
import streamlit as st

st.title("LinkedIn → Bright Data (Hardcoded)")

# ====== Hardcoded creds ======
BRIGHTDATA_API_KEY = "8b81c258b672bca0a33d06f16629919e80e1facf10074179de06e476d66b2c72"
DATASET_ID = "gd_l1viktl72bvl7bjuj0"
# ============================

def normalize_url(x: str) -> str:
    x = (x or "").strip()
    if not x:
        return ""
    if x.startswith("http"):
        return x
    return f"https://www.linkedin.com/in/{x.strip('/').split('?')[0]}/"

def clean_payload(obj):
    """
    Remove any snapshot-related noise before showing.
    Works for dicts or lists.
    """
    SNAP_KEYS = {"snapshot_id", "snapshot_url", "snapshots", "dataset_id"}
    if isinstance(obj, dict):
        return {k: clean_payload(v) for k, v in obj.items() if k not in SNAP_KEYS}
    if isinstance(obj, list):
        return [clean_payload(x) for x in obj]
    return obj

def scrape_profile(url: str):
    headers = {"Authorization": f"Bearer {BRIGHTDATA_API_KEY}", "Content-Type": "application/json"}
    params = {"dataset_id": DATASET_ID}
    body = {"input": [{"url": url}]}

    # 1) Try synchronous scrape
    r = requests.post(
        "https://api.brightdata.com/datasets/v3/scrape",
        params=params, headers=headers, json=body, timeout=60
    )

    if r.status_code == 200:
        return clean_payload(r.json())

    # 2) If not ready, Bright Data returns 202 with snapshot id. We poll quietly.
    if r.status_code == 202:
        data = r.json()
        snap = data.get("snapshot_id")
        if not snap:
            raise RuntimeError(f"202 without snapshot_id: {data}")
        snap_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snap}"

        # Poll up to ~90s, no snapshot info shown to user
        for _ in range(45):
            g = requests.get(snap_url, params={"format": "json"}, headers=headers, timeout=30)
            if g.status_code == 202:
                time.sleep(2)
                continue
            if g.status_code == 200:
                return clean_payload(g.json())
            raise RuntimeError(f"Snapshot error {g.status_code}: {g.text[:400]}")
        raise TimeoutError("Timed out waiting for result")

    # 3) Any other status -> basic error
    raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")

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
                st.error(f"{e}")

# pages/1_🔁_Data_Generator.py
import time
from datetime import datetime, timezone
from typing import Optional
import numpy as np
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Data Generator • Current vs Quick Ratio", page_icon="🔁")
st.title("🔁 Synthetic Data Generator")
st.caption("Writes one fresh record every 30 seconds to Google Sheets (row 2): current assets, current liabilities, and inventory.")

INTERVAL_SECONDS = 30

# ---- Google Sheets helpers ---------------------------------------------------
def get_gspread_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

def open_ws(client: gspread.Client) -> gspread.Worksheet:
    ss = client.open_by_key(st.secrets["gsheet_id"])
    ws_name = st.secrets.get("gsheet_worksheet", "latest")
    try:
        return ss.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=ws_name, rows=10, cols=6)
        ws.update("A1:D1", [["timestamp_utc", "current_assets", "current_liabilities", "inventory"]])
        return ws

def write_single_row(record: dict) -> None:
    client = get_gspread_client()
    ws = open_ws(client)
    # Ensure header exists
    ws.update("A1:D1", [["timestamp_utc", "current_assets", "current_liabilities", "inventory"]])
    # Overwrite row 2 with the latest values
    ws.update("A2:D2", [[
        record["timestamp_utc"],
        record["current_assets"],
        record["current_liabilities"],
        record["inventory"],
    ]])

def read_latest() -> Optional[pd.DataFrame]:
    client = get_gspread_client()
    ws = open_ws(client)
    data = ws.get_values("A1:D2")
    if len(data) < 2 or len(data[1]) < 4:
        return None
    return pd.DataFrame([data[1]], columns=data[0])

# ---- Data synth --------------------------------------------------------------
def generate_plausible_values(rng: np.random.Generator) -> dict:
    # Current Assets ~ 50k–250k, skewed positive
    ca = float(rng.lognormal(mean=11.0, sigma=0.35))
    ca = float(np.clip(ca, 50_000, 250_000))
    # Inventory 10%–60% of CA
    inv_prop = float(rng.uniform(0.10, 0.60))
    inv = inv_prop * ca
    # Current Liabilities 30%–110% of CA
    cl_prop = float(rng.uniform(0.30, 1.10))
    cl = cl_prop * ca
    inv = min(inv, ca)
    cl = max(cl, 1.0)
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "current_assets": round(ca, 2),
        "current_liabilities": round(cl, 2),
        "inventory": round(inv, 2),
    }

# ---- Timing -----------------------------------------------------------------
if "last_generate_ts" not in st.session_state:
    st.session_state.last_generate_ts = 0.0

def due(now: float) -> bool:
    return (now - st.session_state.last_generate_ts) >= INTERVAL_SECONDS

# ---- UI ---------------------------------------------------------------------
c1, c2 = st.columns([1, 3])
manual = c1.button("🔄 Generate now")

rng = np.random.default_rng()
now = time.time()

if manual or due(now):
    rec = generate_plausible_values(rng)
    write_single_row(rec)
    st.session_state.last_generate_ts = now

latest = read_latest()

st.subheader("Latest Emission")
if latest is not None and not latest.empty:
    row = latest.iloc[0]
    ca = float(row["current_assets"])
    cl = float(row["current_liabilities"])
    inv = float(row["inventory"])

    m1, m2, m3 = st.columns(3)
    m1.metric("Current Assets (£)", f"{ca:,.2f}")
    m2.metric("Current Liabilities (£)", f"{cl:,.2f}")
    m3.metric("Inventory (£)", f"{inv:,.2f}")

    st.caption(f"Last updated: {row['timestamp_utc']} (UTC)")
else:
    st.info("No data yet — generating the first record...")

elapsed = time.time() - st.session_state.last_generate_ts
remaining = max(0, INTERVAL_SECONDS - int(elapsed))
st.write(f"Next auto-generate in **{remaining}s**.")
st.progress(1.0 - (remaining / INTERVAL_SECONDS) if INTERVAL_SECONDS else 1.0)

time.sleep(1)
st.rerun()

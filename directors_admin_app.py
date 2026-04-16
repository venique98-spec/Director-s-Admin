import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Director's Admin", layout="wide")

# ---------------- CONFIG ----------------
SHEET_ID = st.secrets["GSHEET_ID"]
SERVING_BASE_TAB = "ServingBase"
RESPONSES_TAB = "Responses"
MAPPING_TAB = "Mapping sheet"
CHANGES_TAB = "Changes"

PRIORITY_GROUPS = {
    "First Priority": ["1A","1B","1C","1D","1E"],
    "Second Priority": ["2A","2B","2C","2D","2E"],
    "Third Priority": ["3A","3B","3C","3D","3E"],
    "Fourth Priority": ["4A","4B"],
    "Fifth Priority": ["5"],
}

CAMPUS_MAP = {
    "TGB": "Tygerberg",
    "UC": "Unit City",
    "LYN": "Lynwood",
    "BRK": "Brooklyn",
    "POL": "Polokwane",
    "NEL": "Nelspruit",
}

# ---------------- HELPERS ----------------
def normalize_text(v): return "" if v is None else str(v).strip()
def normalized_key(v): return normalize_text(v).lower()
def is_blank(v): return normalize_text(v) in ["", "n/a", "na", "none"]

def get_target_month():
    return (pd.Timestamp.now() + pd.DateOffset(months=1)).strftime("%Y-%m")

# ---------------- GOOGLE ----------------
def get_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def read_tab(name):
    sheet = get_client().open_by_key(SHEET_ID).worksheet(name)
    return pd.DataFrame(sheet.get_all_records())

def append_change(director, text):
    sheet = get_client().open_by_key(SHEET_ID).worksheet(CHANGES_TAB)
    sheet.append_row([
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        director,
        text
    ])

# ---------------- BUILD TABLES ----------------
def build_priority_table(row, mapping):
    rows = []

    campus = CAMPUS_MAP.get(row.get("Primary Campus",""), row.get("Primary Campus",""))
    if campus:
        rows.append(f"<tr><td width='40%'><b>Primary Campus</b></td><td>{campus}</td></tr>")

    for name, cols in PRIORITY_GROUPS.items():
        vals = []
        for c in cols:
            v = row.get(c,"")
            if not is_blank(v):
                vals.append(mapping.get(v, v))
        if vals:
            rows.append(f"<tr><td><b>{name}</b></td><td>{'<br>'.join(vals)}</td></tr>")

    return f"""
    <table style='width:100%; table-layout:fixed'>
    {''.join(rows)}
    </table>
    """

def build_status(resp, target):
    if resp is None:
        return "<div style='background:#fde8e8;padding:10px;color:#991b1b;font-weight:600'>No submission found</div>"

    month = resp.get("Availability month","")

    if month == target:
        return f"<div style='background:#dcfce7;padding:10px;color:#166534;font-weight:600'>Latest response submitted for availability month: {month}</div>"

    return f"<div style='background:#fde8e8;padding:10px;color:#991b1b;font-weight:600'>Wrong month submitted: {month}</div>"

def build_response_table(resp):
    if resp is None:
        return ""

    rows = []
    for k,v in resp.items():
        if k.lower() in ["timestamp","director","serving girl","availability month"]:
            continue
        if is_blank(v):
            continue

        if k.lower()=="reason":
            rows.append(f"<tr><td><b>{k}</b></td><td style='background:#fde8e8;color:#991b1b'><b>{v}</b></td></tr>")
        else:
            rows.append(f"<tr><td>{k}</td><td>{v}</td></tr>")

    return f"""
    <table style='width:100%; table-layout:fixed'>
    <tr><th width='40%'>Date</th><th width='60%'>Availability month: {resp.get("Availability month","")}</th></tr>
    {''.join(rows)}
    </table>
    """

# ---------------- MAIN ----------------
st.title("Director's Admin")

serving = read_tab(SERVING_BASE_TAB)
responses = read_tab(RESPONSES_TAB)
mapping_df = read_tab(MAPPING_TAB)

mapping = dict(zip(mapping_df["Shortened Name"], mapping_df["Display Name"]))

director = st.selectbox("Select a director", sorted(serving["Director"].unique()))
st.subheader(f"Director: {director}")

target = get_target_month()

latest = responses.sort_values("Timestamp", ascending=False)\
                  .drop_duplicates("Serving Girl")

for _, row in serving[serving["Director"]==director].iterrows():
    girl = row["Serving Girl"]
    resp = latest[latest["Serving Girl"]==girl]
    resp = None if resp.empty else resp.iloc[0]

    with st.expander(girl):

        # WATERMARK
        st.markdown("""
        <div style="position:absolute;opacity:0.07;font-size:40px;transform:rotate(-20deg)">
        Don't share with Serving girls
        </div>
        """, unsafe_allow_html=True)

        st.markdown(build_priority_table(row, mapping), unsafe_allow_html=True)
        st.markdown(build_status(resp, target), unsafe_allow_html=True)
        st.markdown(build_response_table(resp), unsafe_allow_html=True)

# ---------------- REPORT SECTION ----------------
st.markdown("---")
st.markdown("## 📌 Report a change")

change = st.text_area("Describe the change")

if st.button("Submit change"):
    if change.strip():
        append_change(director, change)
        st.success("Submitted ✅")
    else:
        st.warning("Enter something first")

import re
from typing import Dict, List, Optional, Tuple

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

# =========================================================
# Director's Admin
# =========================================================
# Expected Google Sheets tabs:
# 1. ServingBase
# 2. Responses
# 3. Mapping sheet
#
# Required columns in ServingBase:
# Director, Serving Girl, Primary Campus, Secondary Campus, Group,
# 1A,1B,1C,1D,1E,2A,2B,2C,2D,2E,3A,3B,3C,3D,3E,4A,4B,5
#
# Required columns in Mapping sheet:
# Shortened Name, Display Name
#
# Responses tab:
# Must still contain the serving girl name column and a timestamp column.
# The code tries a few common header variations automatically.
# =========================================================

st.set_page_config(page_title="Director's Admin", layout="wide")

# -----------------------------
# Configuration
# -----------------------------
SHEET_ID = st.secrets.get("GSHEET_ID", "")
SERVING_BASE_TAB = "ServingBase"
RESPONSES_TAB = "Responses"
MAPPING_TAB = "Mapping sheet"

PRIORITY_GROUPS = {
    "First Priority": ["1A", "1B", "1C", "1D", "1E"],
    "Second Priority": ["2A", "2B", "2C", "2D", "2E"],
    "Third Priority": ["3A", "3B", "3C", "3D", "3E"],
    "Fourth Priority": ["4A", "4B"],
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

UNKNOWN_ROLE_MESSAGE = "Contact Venique to add this role to the mappings list"

# Columns that should not be repeated in the latest response answer list
RESPONSE_EXCLUDE_COLUMNS = {
    "timestamp",
    "time stamp",
    "serving girl",
    "servinggirl",
    "name",
    "director",
    "availability month",
    "availabilitymonth",
}


# -----------------------------
# Utilities
# -----------------------------
def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalized_key(value) -> str:
    value = normalize_text(value).lower()
    value = re.sub(r"\s+", " ", value)
    return value


def is_blank_or_na(value) -> bool:
    text = normalize_text(value)
    return text == "" or normalized_key(text) in {"n/a", "na", "none", "null", "nan", "-"}


def prettify_label(label: str) -> str:
    label = normalize_text(label)
    return label


def parse_timestamp(value) -> Optional[pd.Timestamp]:
    if is_blank_or_na(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed


def find_column(df: pd.DataFrame, candidates: List[str], required: bool = True) -> Optional[str]:
    normalized_to_actual = {normalized_key(col): col for col in df.columns}
    for candidate in candidates:
        if normalized_key(candidate) in normalized_to_actual:
            return normalized_to_actual[normalized_key(candidate)]
    if required:
        raise KeyError(f"Could not find required column. Tried: {candidates}")
    return None


def safe_get(row: pd.Series, column_name: str):
    return row[column_name] if column_name in row.index else ""


def map_campus(code: str) -> str:
    code = normalize_text(code)
    if is_blank_or_na(code):
        return ""
    return CAMPUS_MAP.get(code.upper(), code)


def split_multi_role_codes(value: str) -> List[str]:
    text = normalize_text(value)
    if is_blank_or_na(text):
        return []
    parts = re.split(r"\s*&\s*|\s*,\s*|\s*/\s*|\s*\+\s*", text)
    cleaned = [p.strip() for p in parts if p.strip()]
    return cleaned


# -----------------------------
# Google Sheets access
# -----------------------------
def get_gspread_client() -> gspread.Client:
    if not SHEET_ID:
        raise ValueError("Missing GSHEET_ID in Streamlit secrets.")

    service_account_info = st.secrets.get("gcp_service_account")
    if not service_account_info:
        raise ValueError("Missing gcp_service_account in Streamlit secrets.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return gspread.authorize(credentials)


@st.cache_resource(show_spinner=False)
def open_workbook():
    client = get_gspread_client()
    return client.open_by_key(SHEET_ID)


@st.cache_data(ttl=60, show_spinner=False)
def read_tab(tab_name: str) -> pd.DataFrame:
    workbook = open_workbook()
    worksheet = workbook.worksheet(tab_name)
    records = worksheet.get_all_records()
    df = pd.DataFrame(records)

    if df.empty:
        headers = worksheet.row_values(1)
        if headers:
            df = pd.DataFrame(columns=headers)

    # Strip header whitespace
    df.columns = [normalize_text(col) for col in df.columns]
    return df


# -----------------------------
# Data prep
# -----------------------------
def load_mapping_dict(mapping_df: pd.DataFrame) -> Dict[str, str]:
    short_col = find_column(mapping_df, ["Shortened Name", "ShortenedName", "Short Name", "Code"])
    display_col = find_column(mapping_df, ["Display Name", "DisplayName", "Role Name", "Full Name"])

    mapping = {}
    for _, row in mapping_df.iterrows():
        short_name = normalize_text(row[short_col]).upper()
        display_name = normalize_text(row[display_col])
        if short_name and display_name:
            mapping[short_name] = display_name
    return mapping


def prepare_servingbase(serving_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    director_col = find_column(serving_df, ["Director"])
    serving_girl_col = find_column(serving_df, ["Serving Girl", "ServingGirl", "Name"])
    primary_campus_col = find_column(serving_df, ["Primary Campus", "Primary Campu", "PrimaryCampus"], required=False)
    secondary_campus_col = find_column(serving_df, ["Secondary Campus", "Secondary Camp", "SecondaryCampus"], required=False)
    group_col = find_column(serving_df, ["Group"], required=False)

    renamed = serving_df.copy()
    rename_map = {
        director_col: "Director",
        serving_girl_col: "Serving Girl",
    }
    if primary_campus_col:
        rename_map[primary_campus_col] = "Primary Campus"
    if secondary_campus_col:
        rename_map[secondary_campus_col] = "Secondary Campus"
    if group_col:
        rename_map[group_col] = "Group"

    renamed = renamed.rename(columns=rename_map)

    for heading, cols in PRIORITY_GROUPS.items():
        for c in cols:
            if c not in renamed.columns:
                renamed[c] = ""

    if "Primary Campus" not in renamed.columns:
        renamed["Primary Campus"] = ""
    if "Secondary Campus" not in renamed.columns:
        renamed["Secondary Campus"] = ""
    if "Group" not in renamed.columns:
        renamed["Group"] = ""

    return renamed, rename_map


def prepare_latest_responses(responses_df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str]:
    if responses_df.empty:
        return responses_df.copy(), "Serving Girl", "timestamp"

    serving_girl_col = find_column(
        responses_df,
        ["Serving Girl", "ServingGirl", "Name", "Serving girl name"],
    )
    timestamp_col = find_column(
        responses_df,
        ["timestamp", "Timestamp", "Time stamp", "Submitted At", "Submission Timestamp"],
    )

    df = responses_df.copy()
    df["__serving_girl_key"] = df[serving_girl_col].apply(lambda x: normalized_key(x))
    df["__timestamp_parsed"] = df[timestamp_col].apply(parse_timestamp)
    df = df.dropna(subset=["__serving_girl_key"])
    df = df[df["__serving_girl_key"] != ""]
    df = df.sort_values(by="__timestamp_parsed", ascending=False, na_position="last")
    latest = df.drop_duplicates(subset=["__serving_girl_key"], keep="first")

    return latest, serving_girl_col, timestamp_col


def map_role_codes_to_display(raw_value: str, mapping_dict: Dict[str, str]) -> List[str]:
    codes = split_multi_role_codes(raw_value)
    if not codes:
        return []

    display_values = []
    for code in codes:
        upper_code = code.upper()
        if upper_code in mapping_dict:
            display_values.append(mapping_dict[upper_code])
        else:
            display_values.append(UNKNOWN_ROLE_MESSAGE)
    return display_values


def build_priority_sections(row: pd.Series, mapping_dict: Dict[str, str]) -> Dict[str, List[str]]:
    sections = {}
    for heading, cols in PRIORITY_GROUPS.items():
        values = []
        for col in cols:
            if col not in row.index:
                continue
            raw_value = safe_get(row, col)
            if is_blank_or_na(raw_value):
                continue
            values.extend(map_role_codes_to_display(raw_value, mapping_dict))
        values = [v for v in values if not is_blank_or_na(v)]
        if values:
            sections[heading] = values
    return sections


def extract_response_answers(response_row: pd.Series) -> List[Tuple[str, str]]:
    items = []
    for col in response_row.index:
        if str(col).startswith("__"):
            continue
        if normalized_key(col) in RESPONSE_EXCLUDE_COLUMNS:
            continue
        value = response_row[col]
        if is_blank_or_na(value):
            continue
        if normalized_key(value) == "no":
            continue
        items.append((prettify_label(col), normalize_text(value)))
    return items


def get_availability_month(response_row: Optional[pd.Series]) -> str:
    if response_row is None:
        return ""

    for col in response_row.index:
        if normalized_key(col) in {"availability month", "availabilitymonth"}:
            return normalize_text(response_row[col])
    return ""


def get_target_availability_month() -> str:
    now_local = pd.Timestamp.now(tz="Africa/Johannesburg")
    return (now_local + pd.DateOffset(months=1)).strftime("%Y-%m")


def is_current_month_submission(response_row: Optional[pd.Series], target_month: str) -> bool:
    availability_month = get_availability_month(response_row)
    return availability_month == target_month


# -----------------------------
# UI helpers
# -----------------------------
def render_serving_girl_card(serving_row: pd.Series, latest_response_row: Optional[pd.Series], mapping_dict: Dict[str, str], target_month: str):
    serving_girl = normalize_text(serving_row["Serving Girl"])
    director = normalize_text(serving_row["Director"])

    availability_month = get_availability_month(latest_response_row)
    has_current_submission = is_current_month_submission(latest_response_row, target_month)

    if latest_response_row is None:
        status_html = "<div style='background-color:#fde8e8;color:#991b1b;padding:10px 12px;border-radius:8px;margin:8px 0 10px 0;font-weight:600;'>No submission found for this serving girl.</div>"
    elif has_current_submission:
        status_html = f"<div style='background-color:#dcfce7;color:#166534;padding:8px 10px;border-radius:6px;margin:4px 0 4px 0;font-weight:600;'>Latest response submitted for availability month: {availability_month}</div>"
    else:
        status_html = f"<div style='background-color:#fde8e8;color:#991b1b;padding:10px 12px;border-radius:8px;margin:8px 0 10px 0;font-weight:600;'>Latest response found for availability month {availability_month or 'Unknown'}, not for the current target month ({target_month}).</div>"

    primary_campus = map_campus(safe_get(serving_row, "Primary Campus"))
    secondary_campus = map_campus(safe_get(serving_row, "Secondary Campus"))
    group_value = normalize_text(safe_get(serving_row, "Group"))
    priority_sections = build_priority_sections(serving_row, mapping_dict)

    with st.expander(serving_girl, expanded=False):
        if primary_campus:
            st.write(f"**Primary Campus:** {primary_campus}")
        if secondary_campus:
            st.write(f"**Secondary Campus:** {secondary_campus}")
        if serving_girl != director and not is_blank_or_na(group_value):
            st.write(f"**Group:** {group_value}")

        st.markdown(status_html, unsafe_allow_html=True)

        # Show priorities first
        for heading, values in priority_sections.items():
            st.markdown(f"**{heading}**", unsafe_allow_html=True)
            for value in values:
                st.markdown(f"<div style='margin-left:10px;margin-bottom:2px;'>• {value}</div>", unsafe_allow_html=True)

        # Then show latest response dropdown
        if latest_response_row is not None:
            with st.expander("View latest response", expanded=False):
                if availability_month:
                    st.write(f"**Availability month:** {availability_month}")

                response_items = extract_response_answers(latest_response_row)
                if response_items:
                    for label, value in response_items:
                        st.write(f"**{label}:** {value}")
                else:
                    st.info("No Yes dates or additional response details were available.")


# -----------------------------
# Main app
# -----------------------------
def main():
    st.title("Director's Admin")
    st.caption("View each serving girl under a director, their scheduled priorities, and the latest response submitted.")

    try:
        serving_df_raw = read_tab(SERVING_BASE_TAB)
        responses_df_raw = read_tab(RESPONSES_TAB)
        mapping_df_raw = read_tab(MAPPING_TAB)
    except Exception as e:
        st.error(f"Could not read Google Sheets data: {e}")
        st.stop()

    if serving_df_raw.empty:
        st.warning("The ServingBase sheet is empty.")
        st.stop()

    try:
        serving_df, _ = prepare_servingbase(serving_df_raw)
        mapping_dict = load_mapping_dict(mapping_df_raw) if not mapping_df_raw.empty else {}
        latest_responses_df, _, _ = prepare_latest_responses(responses_df_raw)
        target_month = get_target_availability_month()
    except Exception as e:
        st.error(f"There is a setup issue in the sheet structure: {e}")
        st.stop()

    serving_df = serving_df.copy()
    serving_df["__director_key"] = serving_df["Director"].apply(normalized_key)
    serving_df["__serving_girl_key"] = serving_df["Serving Girl"].apply(normalized_key)

    director_options = sorted(
        [d for d in serving_df["Director"].dropna().astype(str).map(str.strip).unique().tolist() if d.strip()]
    )

    if not director_options:
        st.warning("No directors were found in the ServingBase sheet.")
        st.stop()

    selected_director = st.selectbox("Select a director", director_options)
    selected_key = normalized_key(selected_director)

    director_rows = serving_df[serving_df["__director_key"] == selected_key].copy()
    director_rows = director_rows.sort_values(by=["Serving Girl"], ascending=True)

    st.subheader(f"Director: {selected_director}")

    if director_rows.empty:
        st.info("No serving girls were found for this director.")
        st.stop()

    if latest_responses_df.empty:
            latest_lookup = {}
    if not latest_responses_df.empty:
        latest_lookup = {
            row["__serving_girl_key"]: row
            for _, row in latest_responses_df.iterrows()
        }

    for _, row in director_rows.iterrows():
        sg_key = row["__serving_girl_key"]
        latest_response_row = latest_lookup.get(sg_key)
        render_serving_girl_card(row, latest_response_row, mapping_dict, target_month)


if __name__ == "__main__":
    main()

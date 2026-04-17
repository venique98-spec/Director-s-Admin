import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Director's Admin", layout="wide")

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
SHEET_ID = st.secrets.get("GSHEET_ID", "")
SERVING_BASE_TAB = "ServingBase"
RESPONSES_TAB = "Responses"
MAPPING_TAB = "Mapping sheet"
CHANGES_TAB = "Changes"

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

RESPONSE_EXCLUDE_COLUMNS = {
    "timestamp",
    "time stamp",
    "director",
    "serving girl",
    "servinggirl",
    "name",
    "availability month",
    "availabilitymonth",
}

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalized_key(value) -> str:
    text = normalize_text(value).lower()
    text = re.sub(r"\s+", " ", text)
    return text


def is_blank_or_na(value) -> bool:
    text = normalize_text(value)
    return text == "" or normalized_key(text) in {"n/a", "na", "none", "null", "nan", "-"}


def parse_timestamp(value) -> Optional[pd.Timestamp]:
    if is_blank_or_na(value):
        return None
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def find_column(df: pd.DataFrame, candidates: List[str], required: bool = True) -> Optional[str]:
    lookup = {normalized_key(col): col for col in df.columns}
    for candidate in candidates:
        if normalized_key(candidate) in lookup:
            return lookup[normalized_key(candidate)]
    if required:
        raise KeyError(f"Could not find required column. Tried: {candidates}")
    return None


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
    return [part.strip() for part in parts if part.strip()]


def get_target_availability_month() -> str:
    now_local = pd.Timestamp.now(tz="Africa/Johannesburg")
    return (now_local + pd.DateOffset(months=1)).strftime("%Y-%m")


def get_availability_month(response_row: Optional[pd.Series]) -> str:
    if response_row is None:
        return ""
    for col in response_row.index:
        if normalized_key(col) in {"availability month", "availabilitymonth"}:
            return normalize_text(response_row[col])
    return ""


def is_current_month_submission(response_row: Optional[pd.Series], target_month: str) -> bool:
    return get_availability_month(response_row) == target_month


# --------------------------------------------------
# GOOGLE SHEETS
# --------------------------------------------------
def get_gspread_client() -> gspread.Client:
    if not SHEET_ID:
        raise ValueError("Missing GSHEET_ID in Streamlit secrets.")

    service_account_info = st.secrets.get("gcp_service_account")
    if not service_account_info:
        raise ValueError("Missing gcp_service_account in Streamlit secrets.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
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

    data = worksheet.get_all_values()
    if not data:
        return pd.DataFrame()

    headers = data[0]
    rows = data[1:]

    # Fix duplicate headers
    seen = {}
    clean_headers = []
    for h in headers:
        h_clean = normalize_text(h)
        if h_clean in seen:
            seen[h_clean] += 1
            h_clean = f"{h_clean}_{seen[h_clean]}"
        else:
            seen[h_clean] = 0
        clean_headers.append(h_clean)

    df = pd.DataFrame(rows, columns=clean_headers)
    return df


def append_change_request(director: str, change_text: str) -> None:
    workbook = open_workbook()
    worksheet = workbook.worksheet(CHANGES_TAB)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    worksheet.append_row([timestamp, director, change_text], value_input_option="USER_ENTERED")
    read_tab.clear()


# --------------------------------------------------
# DATA PREP
# --------------------------------------------------
def load_mapping_dict(mapping_df: pd.DataFrame) -> Dict[str, str]:
    if mapping_df.empty:
        return {}

    short_col = find_column(mapping_df, ["Shortened Name", "ShortenedName", "Short Name", "Code"])
    display_col = find_column(mapping_df, ["Display Name", "DisplayName", "Role Name", "Full Name"])

    mapping = {}
    for _, row in mapping_df.iterrows():
        short_name = normalize_text(row[short_col]).upper()
        display_name = normalize_text(row[display_col])
        if short_name and display_name:
            mapping[short_name] = display_name
    return mapping


def prepare_servingbase(serving_df: pd.DataFrame) -> pd.DataFrame:
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

    for col in ["Primary Campus", "Secondary Campus", "Group"]:
        if col not in renamed.columns:
            renamed[col] = ""

    for cols in PRIORITY_GROUPS.values():
        for col in cols:
            if col not in renamed.columns:
                renamed[col] = ""

    renamed["__director_key"] = renamed["Director"].apply(normalized_key)
    renamed["__serving_girl_key"] = renamed["Serving Girl"].apply(normalized_key)
    return renamed


def prepare_latest_responses(responses_df: pd.DataFrame) -> pd.DataFrame:
    if responses_df.empty:
        return pd.DataFrame()

    serving_girl_col = find_column(responses_df, ["Serving Girl", "ServingGirl", "Name", "Serving girl name"])
    timestamp_col = find_column(responses_df, ["timestamp", "Timestamp", "Time stamp", "Submitted At", "Submission Timestamp"])

    df = responses_df.copy()
    df["__serving_girl_key"] = df[serving_girl_col].apply(normalized_key)
    df["__timestamp_parsed"] = df[timestamp_col].apply(parse_timestamp)
    df = df[df["__serving_girl_key"] != ""]
    df = df.sort_values(by="__timestamp_parsed", ascending=False, na_position="last")
    latest = df.drop_duplicates(subset=["__serving_girl_key"], keep="first")
    return latest


def map_role_codes_to_display(raw_value: str, mapping_dict: Dict[str, str]) -> List[str]:
    codes = split_multi_role_codes(raw_value)
    if not codes:
        return []

    results = []
    for code in codes:
        code_upper = code.upper()
        results.append(mapping_dict.get(code_upper, UNKNOWN_ROLE_MESSAGE))
    return results


def build_priority_sections(row: pd.Series, mapping_dict: Dict[str, str]) -> Dict[str, List[str]]:
    sections = {}
    for heading, cols in PRIORITY_GROUPS.items():
        values = []
        for col in cols:
            raw_value = normalize_text(row.get(col, ""))
            if is_blank_or_na(raw_value):
                continue
            values.extend(map_role_codes_to_display(raw_value, mapping_dict))
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
        items.append((normalize_text(col), normalize_text(value)))
    return items


# --------------------------------------------------
# RENDERING
# --------------------------------------------------
def build_priority_table_html(serving_row: pd.Series, mapping_dict: Dict[str, str]) -> str:
    primary_campus = map_campus(serving_row.get("Primary Campus", ""))
    secondary_campus = map_campus(serving_row.get("Secondary Campus", ""))
    group_value = normalize_text(serving_row.get("Group", ""))
    director = normalize_text(serving_row.get("Director", ""))
    serving_girl = normalize_text(serving_row.get("Serving Girl", ""))
    priority_sections = build_priority_sections(serving_row, mapping_dict)

    rows = []
    if primary_campus:
        rows.append(
            f"<tr><td style='padding:8px 14px; font-weight:600; width:40%;'>Primary Campus</td>"
            f"<td style='padding:8px 14px; width:60%;'>{primary_campus}</td></tr>"
        )
    if secondary_campus:
        rows.append(
            f"<tr><td style='padding:8px 14px; font-weight:600; width:40%;'>Secondary Campus</td>"
            f"<td style='padding:8px 14px; width:60%;'>{secondary_campus}</td></tr>"
        )
    if serving_girl != director and not is_blank_or_na(group_value):
        rows.append(
            f"<tr><td style='padding:8px 14px; font-weight:600; width:40%;'>Group</td>"
            f"<td style='padding:8px 14px; width:60%;'>{group_value}</td></tr>"
        )

    for heading, values in priority_sections.items():
        joined_values = "<br>".join(values)
        rows.append(
            f"<tr><td style='padding:8px 14px; font-weight:600; width:40%;'>{heading}</td>"
            f"<td style='padding:8px 14px; width:60%;'>{joined_values}</td></tr>"
        )

    if not rows:
        return ""

    return f"""
    <table style='width:100%; border-collapse:separate; border-spacing:0 0; table-layout:fixed; margin-bottom:10px;'>
        <tbody>
            {''.join(rows)}
        </tbody>
    </table>
    """


def build_status_html(response_row: Optional[pd.Series], target_month: str) -> str:
    availability_month = get_availability_month(response_row)

    if response_row is None:
        return (
            "<div style='background-color:#fde8e8;color:#991b1b;padding:10px 12px;"
            "border-radius:8px;margin:8px 0 10px 0;font-weight:600;'>"
            "No submission found for this serving girl.</div>"
        )

    if is_current_month_submission(response_row, target_month):
        return (
            f"<div style='background-color:#dcfce7;color:#166534;padding:10px 12px;"
            f"border-radius:8px;margin:8px 0 10px 0;font-weight:600;'>"
            f"Latest response submitted for availability month: {availability_month}</div>"
        )

    return (
        f"<div style='background-color:#fde8e8;color:#991b1b;padding:10px 12px;"
        f"border-radius:8px;margin:8px 0 10px 0;font-weight:600;'>"
        f"Latest response found for availability month {availability_month or 'Unknown'}, "
        f"not for the current target month ({target_month}).</div>"
    )


def build_response_table_html(response_row: Optional[pd.Series]) -> str:
    if response_row is None:
        return ""

    response_items = extract_response_answers(response_row)
    if not response_items:
        return "<div style='margin-top:8px;'>No response details available.</div>"

    availability_month = get_availability_month(response_row)
    header_text = f"Availability month: {availability_month}" if availability_month else "Availability"

    rows = []
    for label, value in response_items:
        if normalized_key(label) == "reason":
            rows.append(
                f"<tr>"
                f"<td style='padding:8px 14px; width:40%; color:#991b1b; font-weight:600;'>Reason</td>"
                f"<td style='padding:8px 14px; width:60%; background-color:#fde8e8; color:#991b1b; font-weight:600;'>{value}</td>"
                f"</tr>"
            )
        else:
            rows.append(
                f"<tr>"
                f"<td style='padding:8px 14px; width:40%;'>{label}</td>"
                f"<td style='padding:8px 14px; width:60%;'>{value}</td>"
                f"</tr>"
            )

    return f"""
    <table style='width:100%; border-collapse:separate; border-spacing:0 0; table-layout:fixed;'>
        <thead>
            <tr>
                <th style='text-align:left; padding:8px 14px; border-bottom:1px solid #e5e7eb; width:40%;'>Date</th>
                <th style='text-align:left; padding:8px 14px; border-bottom:1px solid #e5e7eb; width:60%;'>{header_text}</th>
            </tr>
        </thead>
        <tbody>
            {''.join(rows)}
        </tbody>
    </table>
    """


def render_serving_girl_card(serving_row: pd.Series, latest_response_row: Optional[pd.Series], mapping_dict: Dict[str, str], target_month: str):
    serving_girl = normalize_text(serving_row["Serving Girl"])

    with st.expander(serving_girl, expanded=False):
        priority_table_html = build_priority_table_html(serving_row, mapping_dict)
        if priority_table_html:
            st.markdown(priority_table_html, unsafe_allow_html=True)

        st.markdown(build_status_html(latest_response_row, target_month), unsafe_allow_html=True)

        response_table_html = build_response_table_html(latest_response_row)
        if response_table_html:
            st.markdown(response_table_html, unsafe_allow_html=True)


# --------------------------------------------------
# MAIN
# --------------------------------------------------
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
        serving_df = prepare_servingbase(serving_df_raw)
        latest_responses_df = prepare_latest_responses(responses_df_raw)
        mapping_dict = load_mapping_dict(mapping_df_raw)
    except Exception as e:
        st.error(f"There is a setup issue in the sheet structure: {e}")
        st.stop()

    director_options = sorted(
        [d for d in serving_df["Director"].dropna().astype(str).map(str.strip).unique().tolist() if d.strip()]
    )

    if not director_options:
        st.warning("No directors were found in the ServingBase sheet.")
        st.stop()

    selected_director = st.selectbox("Select a director", director_options)
selected_director_key = normalized_key(selected_director)

# Mandatory confirmation checkbox
confirm_key = f"confirm_{selected_director_key}"
confirmed = st.checkbox(
    "I confirm that I will not share this information with any Serving Girl, as it is intended solely for director verification purposes",
    key=confirm_key
)

st.subheader(f"Director: {selected_director}")

# Block access until confirmed
if not confirmed:
    st.warning("Please confirm the declaration above to access serving girls.")
    st.stop()


director_rows = serving_df[serving_df["__director_key"] == selected_director_key].copy()
director_rows = director_rows.sort_values(by=["Serving Girl"], ascending=True)

    latest_lookup = {}
    if not latest_responses_df.empty:
        latest_lookup = {
            row["__serving_girl_key"]: row
            for _, row in latest_responses_df.iterrows()
        }

    target_month = get_target_availability_month()

    for _, row in director_rows.iterrows():
        latest_response_row = latest_lookup.get(row["__serving_girl_key"])
        render_serving_girl_card(row, latest_response_row, mapping_dict, target_month)

    st.markdown("---")
    st.markdown("## 📌 Report a change")
    st.caption("Let us know if something needs to be updated.")

    change_text = st.text_area("Describe the change you want:", height=120, key="report_change_text")

    if st.button("Submit change"):
        if not change_text.strip():
            st.warning("Please enter a description of the change.")
        else:
            try:
                append_change_request(selected_director, change_text.strip())
                st.success("Your change request has been submitted ✅")
            except Exception as e:
                st.error(f"Error saving change: {e}")


if __name__ == "__main__":
    main()

import requests
import polars as pl
import zipfile
import io
from datetime import datetime

import streamlit as st

# -----------------------------
# Helpers
# -----------------------------

def _headers_from_secrets() -> dict:
    """Load auth headers from Streamlit secrets (preferred) or sidebar inputs (fallback)."""
    username = st.secrets.get("username") if hasattr(st, "secrets") else None
    hash_value = st.secrets.get("hash_value") if hasattr(st, "secrets") else None

    # Sidebar overrides (useful for local testing)
    with st.sidebar:
        st.subheader("API Auth")
        username_in = st.text_input("username header", value=username or "")
        hash_in = st.text_input("hash_value header", value=hash_value or "", type="password")

    username = username_in.strip() or username
    hash_value = hash_in.strip() or hash_value

    if not username or not hash_value:
        st.warning("Missing API headers. Add them to .streamlit/secrets.toml or enter them in the sidebar.")

    return {
        "username": username or "",
        "hash_value": hash_value or "",
    }


# streamlit_app.py
# NOTE: This is a line-for-line functional port of your script into Streamlit UI primitives.
# The underlying mechanism (API calls, filtering, grouping, export naming) is preserved.
# Key changes are ONLY: (1) replace input()/print() with Streamlit widgets, (2) use file uploader
# instead of local glob CSV selection, (3) provide download button instead of writing only to disk.
#
# Run: streamlit run streamlit_app.py



pl.Config.set_tbl_rows(100)

# Define HTTP headers (same keys as script)
DEFAULT_HEADERS = _headers_from_secrets()

# API base URL (same)
base_url = "https://bdc.fcc.gov/api/public/map/"

# Default states list (same as your script)
DEFAULT_STATES = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
    "District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota",
    "Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey",
    "New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon",
    "Pennsylvania","Puerto Rico","Rhode Island","South Carolina","South Dakota","Tennessee",
    "Texas","Utah","Vermont","Virginia","West Virginia","Wisconsin","Wyoming","Washington",
]

st.set_page_config(page_title="FCC BDC Availability Export", layout="wide")
st.title("FCC BDC Availability Export (Streamlit)")

# ----------------------------
# Headers UI (same mechanism)
# ----------------------------
with st.expander("HTTP headers", expanded=True):
    username = st.text_input("username", value=DEFAULT_HEADERS["username"])
    hash_value = st.text_input("hash_value", value=DEFAULT_HEADERS["hash_value"], type="password")
headers = {"username": username, "hash_value": hash_value}

# ----------------------------
# Step 1: listAsOfDates (same request)
# ----------------------------
st.header("Step 1 — Get available as-of dates")
if st.button("Fetch dates"):
    r = requests.get(f"{base_url}/listAsOfDates", headers=headers)
    if r.status_code == 200:
        df_dates = pl.DataFrame(r.json()["data"])
        df_avail_dates = df_dates.filter(df_dates["data_type"] == "availability")
        st.session_state["df_avail_dates"] = df_avail_dates
    else:
        st.error("Unable to fetch the dates.")
        st.code(r.text)

df_avail_dates = st.session_state.get("df_avail_dates", None)
as_of_date = None
if df_avail_dates is not None:
    st.write("Available dates (availability):")
    st.dataframe(df_avail_dates.to_pandas())

    # Keep the same expectation: user chooses yyyy-mm-dd
    if "as_of_date" in df_avail_dates.columns:
        date_opts = df_avail_dates["as_of_date"].unique().drop_nulls().sort().to_list()
        as_of_date = st.selectbox("Select date (yyyy-mm-dd)", options=date_opts, index=len(date_opts) - 1)
    else:
        st.warning("No 'as_of_date' column found in response.")

# ----------------------------
# Step 2: Fetch listing for chosen date (same request)
# ----------------------------
st.header("Step 2 — Load availability listing for the selected date")

if st.button("Fetch availability listing", disabled=as_of_date is None):
    r1 = requests.get(f"{base_url}/downloads/listAvailabilityData/{as_of_date}", headers=headers)
    if r1.status_code == 200:
        data = r1.json()["data"]

        # Same preprocessing you do
        for row in data:
            row["provider_id"] = str(row["provider_id"]) if row["provider_id"] is not None else "Blank"
            row["provider_name"] = str(row["provider_name"]) if row["provider_name"] is not None else "Blank"
        for row in data:
            if "speed_tier" in row:
                row["speed_tier"] = str(row["speed_tier"])

        df1 = pl.DataFrame(data)

        df1_fixed_broadband = df1.filter(df1["technology_type"] == "Fixed Broadband")
        df1_fixed_broadband = df1_fixed_broadband.filter(df1_fixed_broadband["category"] == "State")

        tech_list = df1_fixed_broadband["technology_code_desc"].unique().drop_nulls().sort()

        st.session_state["df1_fixed_broadband"] = df1_fixed_broadband
        st.session_state["tech_list"] = tech_list.to_list()

    else:
        st.error("Unable to fetch the data.")
        st.code(r1.text)

df1_fixed_broadband = st.session_state.get("df1_fixed_broadband", None)
tech_list = st.session_state.get("tech_list", [])

if df1_fixed_broadband is not None:
    st.write("Available technology types:")
    st.dataframe(pl.DataFrame({"technology_code_desc": tech_list}).to_pandas())

# ----------------------------
# Step 3: Form (line-by-line like your input() prompts)
# ----------------------------
st.header("Step 3 — Inputs (mirrors your script prompts)")

def _parse_int_list(s: str):
    s = (s or "").strip()
    if not s:
        return []
    return [int(x.strip()) for x in s.split(",") if x.strip()]

with st.form("inputs_form", clear_on_submit=False):
    # States selection with ALL option (explicit requirement)
    select_all_states = st.checkbox("Select ALL states for export", value=True)

    states_input_mode = st.radio(
        "States input mode",
        options=["SKIP (use list in code)", "Enter states manually (comma-separated)"],
        index=0,
        disabled=select_all_states,
        horizontal=True,
    )

    manual_states = st.text_area(
        "States (comma-separated)",
        value="",
        disabled=select_all_states or (states_input_mode == "SKIP (use list in code)"),
    )

    # Tech selection (indices replaced by multiselect, but same outcome: choose tech_of_interest_list)
    tech_of_interest_list = st.multiselect(
        "Technology types of interest",
        options=tech_list,
        default=[],
        disabled=(df1_fixed_broadband is None),
    )

    # Residential-only
    resi_choice = st.selectbox("Residential-only service? (y/n)", options=["y", "n"], index=1)

    # Providers to define location IDs
    include_location_providers = st.selectbox("Specify providers to define location IDs? (y/n)", options=["y", "n"], index=1)
    location_provider_ids_raw = st.text_input("Provider IDs to define location IDs (comma-separated)", value="", disabled=(include_location_providers != "y"))

    # Define provider subset footprint based on technology
    include_provider_subset = st.selectbox("Define a subset of provider footprint by technology? (y/n)", options=["y", "n"], index=1)
    provider_subset_tech = st.multiselect(
        "Subset technology types (used to filter location_ids)",
        options=tech_list,
        default=[],
        disabled=(include_provider_subset != "y"),
    )

    # Exclude providers
    exclude_providers = st.selectbox("Exclude any providers? (y/n)", options=["y", "n"], index=1)
    excluded_provider_ids_raw = st.text_input("Provider IDs to exclude (comma-separated)", value="", disabled=(exclude_providers != "y"))

    # Grouping options
    group_on_speed = st.selectbox("Group on speed tier as well? (y/n)", options=["y", "n"], index=1)
    group_on_technology = st.selectbox("Group purely by technology? (y/n)", options=["y", "n"], index=1)

    # Upload CB list
    cb_choice = st.selectbox("Upload your own list of CB geoids? (y/n)", options=["y", "n"], index=1)
    uploaded_cb = st.file_uploader("Upload CSV with a 'block_geoid' column", type=["csv"], disabled=(cb_choice != "y"))

    submitted = st.form_submit_button("Submit inputs")

if submitted:
    # Resolve states exactly like your script’s SKIP logic + ALL option
    if select_all_states:
        states_of_interest = DEFAULT_STATES
    else:
        if states_input_mode == "SKIP (use list in code)":
            states_of_interest = DEFAULT_STATES
        else:
            states_of_interest = [s.strip() for s in manual_states.split(",") if s.strip()]

    location_provider_ids = _parse_int_list(location_provider_ids_raw) if include_location_providers == "y" else []
    excluded_provider_ids = _parse_int_list(excluded_provider_ids_raw) if exclude_providers == "y" else []
    provider_subset_tech_final = provider_subset_tech if include_provider_subset == "y" else None

    user_cb_geoids = None
    if cb_choice == "y" and uploaded_cb is not None:
        user_cb_geoids = pl.read_csv(uploaded_cb)["block_geoid"].to_list()

    st.session_state["inputs"] = {
        "states_of_interest": states_of_interest,
        "tech_of_interest_list": tech_of_interest_list,
        "resi_choice": resi_choice,
        "location_provider_ids": location_provider_ids,
        "provider_subset_tech": provider_subset_tech_final,
        "excluded_provider_ids": excluded_provider_ids,
        "group_on_speed": group_on_speed,
        "group_on_technology": group_on_technology,
        "user_cb_geoids": user_cb_geoids,
        "as_of_date": as_of_date,
    }
    st.success("Inputs saved. Now run export below.")

# ----------------------------
# Step 4: Run export (same core code)
# ----------------------------
st.header("Step 4 — Run export")

inputs = st.session_state.get("inputs", None)

run_disabled = (
    inputs is None
    or df1_fixed_broadband is None
    or not inputs["tech_of_interest_list"]
    or not inputs["states_of_interest"]
    or inputs["as_of_date"] is None
)

if st.button("Run export", disabled=run_disabled):
    states_of_interest = inputs["states_of_interest"]
    tech_of_interest_list = inputs["tech_of_interest_list"]
    resi_choice = inputs["resi_choice"]
    location_provider_ids = inputs["location_provider_ids"]
    provider_subset_tech = inputs["provider_subset_tech"]
    excluded_provider_ids = inputs["excluded_provider_ids"]
    group_on_speed = inputs["group_on_speed"]
    group_on_technology = inputs["group_on_technology"]
    user_cb_geoids = inputs["user_cb_geoids"]

    # IMPORTANT GUARD:
    # Your original code *will* crash if provider_subset_tech is set but the downloaded raw data
    # doesn't have 'technology_code_desc'. We keep your mechanism intact by preventing execution
    # of that branch unless the column exists (checked after first download).
    #
    # If you want the branch to work universally, that requires a mechanism change (mapping to a
    # column that exists in the raw file), which you explicitly told me not to do.

    dfs_dict = {}
    prog = st.progress(0)
    status = st.empty()

    for i, state in enumerate(states_of_interest, start=1):
        combined_raw_df_for_state = pl.DataFrame()

        for tech_of_interest in tech_of_interest_list:
            df1_filtered = df1_fixed_broadband.filter(df1_fixed_broadband["state_name"] == state)
            df1_filtered = df1_filtered.filter(df1_filtered["technology_code_desc"] == tech_of_interest)

            for row in df1_filtered.iter_rows(named=True):
                file_id = row["file_id"]
                r2 = requests.get(f"{base_url}/downloads/downloadFile/availability/{file_id}/1", headers=headers)
                if r2.status_code == 200:
                    status.write(f"Downloading data for file_id = {file_id} ({tech_of_interest})...")
                    with zipfile.ZipFile(io.BytesIO(r2.content)) as z:
                        with z.open(z.namelist()[0]) as f:
                            df_raw = pl.read_csv(f)
                    combined_raw_df_for_state = combined_raw_df_for_state.vstack(df_raw)
                else:
                    st.warning(f"Could not download data for file_id = {file_id} ({r2.status_code}).")
                    st.code(r2.text)

        # Now apply filtering on the combined raw data (same)
        if resi_choice == "y":
            combined_raw_df_for_state = combined_raw_df_for_state.filter(
                combined_raw_df_for_state["business_residential_code"] != "B"
            )

        # Apply the new logic (same as your code)
        if location_provider_ids:
            loc_ids = combined_raw_df_for_state.filter(
                combined_raw_df_for_state["provider_id"].is_in(location_provider_ids)
            )["location_id"].unique()

            if provider_subset_tech:
                # Guard: only allow if the raw file actually has the column your original code uses
                if "technology_code_desc" not in combined_raw_df_for_state.columns:
                    st.error(
                        'Your selection "Define a subset of a provider\'s footprint based on technology" '
                        'requires the downloaded data to contain a "technology_code_desc" column, but it is missing '
                        "in the downloaded availability file(s). Disable that option or use a file set that includes it."
                    )
                    st.stop()

                loc_ids = combined_raw_df_for_state.filter(
                    (combined_raw_df_for_state["location_id"].is_in(loc_ids))
                    & (combined_raw_df_for_state["technology_code_desc"].is_in(provider_subset_tech))
                )["location_id"].unique()

            combined_raw_df_for_state = combined_raw_df_for_state.filter(
                combined_raw_df_for_state["location_id"].is_in(loc_ids)
            )

        # Exclude the specified providers (same)
        if excluded_provider_ids:
            combined_raw_df_for_state = combined_raw_df_for_state.filter(
                ~combined_raw_df_for_state["provider_id"].is_in(excluded_provider_ids)
            )

        # Group by speed tier if selected (same)
        if group_on_speed == "y":
            combined_raw_df_for_state = combined_raw_df_for_state.group_by(
                ["provider_id", "block_geoid", "location_id"]
            ).agg(pl.col("max_advertised_download_speed").max())

        # Group data by technology only (same branching)
        if group_on_technology == "y":
            combined_raw_df_for_state = combined_raw_df_for_state.group_by(["block_geoid"]).agg(
                pl.col("location_id").n_unique()
            )
        elif group_on_speed == "y":
            combined_raw_df_for_state = combined_raw_df_for_state.group_by(
                ["provider_id", "block_geoid", "max_advertised_download_speed"]
            ).agg(pl.col("location_id").n_unique())
        else:
            combined_raw_df_for_state = combined_raw_df_for_state.group_by(
                ["provider_id", "block_geoid"]
            ).agg(pl.col("location_id").n_unique())

        # Filter with block_geoid column based on the list of CBs (same)
        if user_cb_geoids is not None:
            combined_raw_df_for_state = combined_raw_df_for_state.filter(
                combined_raw_df_for_state["block_geoid"].is_in(user_cb_geoids)
            )

        dfs_dict[state] = combined_raw_df_for_state
        status.write(f"Data for {state} processed.")
        prog.progress(int(i / len(states_of_interest) * 100))

    # Merge data for all states (same)
    df_merged = pl.DataFrame()
    for state_name in dfs_dict:
        df_merged = df_merged.vstack(dfs_dict[state_name])

    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    tech_names_combined = "_".join([tech.replace(" ", "") for tech in tech_of_interest_list])
    out_name = f"{tech_names_combined}_{current_datetime}.csv"

    # Streamlit download (doesn't change output content, only delivery)
    csv_bytes = df_merged.write_csv().encode("utf-8")

    st.success(f"Data output ready: {out_name}")
    st.dataframe(df_merged.head(200).to_pandas())
    st.download_button("Download CSV", data=csv_bytes, file_name=out_name, mime="text/csv")

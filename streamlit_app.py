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


def _block_geoid_prefix_len(level: str) -> int:
    """15-digit Census block GEOID rollups by prefix length."""
    level = (level or "").lower().strip()
    if level == "state":
        return 2
    if level == "county":
        return 5
    if level == "tract":
        return 11
    if level in ("cbg", "block group", "blockgroup"):
        return 12
    # default: block (no rollup)
    return 15


pl.Config.set_tbl_rows(100)

# -----------------------------
# Constants / defaults (same as your script)
# -----------------------------
DEFAULT_STATES = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
    "District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota",
    "Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey",
    "New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon",
    "Pennsylvania","Puerto Rico","Rhode Island","South Carolina","South Dakota","Tennessee",
    "Texas","Utah","Vermont","Virginia","West Virginia","Wisconsin","Wyoming","Washington",
]

# API base URL
base_url = "https://bdc.fcc.gov/api/public/map/"

# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="FCC BDC Availability Export (Polars)", layout="wide")
st.title("FCC BDC Availability Export (Streamlit)")

headers = _headers_from_secrets()

# Helper to parse comma-separated ints (preserving your int conversion behavior)
def parse_int_list(s: str):
    s = (s or "").strip()
    if not s:
        return []
    return [int(x.strip()) for x in s.split(",") if x.strip()]

# Helper to parse comma-separated strings
def parse_str_list(s: str):
    s = (s or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

# -----------------------------
# Step 1: Get as-of dates (same API call)
# -----------------------------
st.subheader("1) Pick an as-of date (availability)")

get_dates = st.button("Fetch available as-of dates")

if get_dates:
    r = requests.get(f"{base_url}/listAsOfDates", headers=headers)
    if r.status_code == 200:
        df_dates = pl.DataFrame(r.json()["data"])
        df_avail_dates = df_dates.filter(df_dates["data_type"] == "availability")

        st.write("Available dates (availability):")
        st.dataframe(df_avail_dates.to_pandas())

        # Provide a selectbox for the date (instead of input)
        # Keep format expectation yyyy-mm-dd.
        date_options = (
            df_avail_dates["as_of_date"].unique().drop_nulls().sort().to_list()
            if "as_of_date" in df_avail_dates.columns
            else []
        )
        st.session_state["date_options"] = date_options
        st.session_state["df_avail_dates"] = df_avail_dates

    else:
        st.error("Unable to fetch the dates.")
        st.code(r.text)

date_options = st.session_state.get("date_options", [])
as_of_date = st.selectbox(
    "Please select the date you are interested in (yyyy-mm-dd)",
    options=date_options,
    index=(len(date_options) - 1) if date_options else None,
    placeholder="Fetch dates first",
    disabled=not bool(date_options),
)

# -----------------------------
# Step 2: States selection (adds ALL option as requested)
# -----------------------------
st.subheader("2) States of interest")

col_a, col_b = st.columns([1, 2], vertical_alignment="top")
with col_a:
    select_all_states = st.checkbox("Select ALL states for export", value=True)

with col_b:
    if select_all_states:
        states_of_interest = DEFAULT_STATES[:]  # same content
        st.info(f"ALL selected ({len(states_of_interest)} states/territories).")
    else:
        # Mirrors your SKIP vs custom input concept, but in Streamlit form:
        states_mode = st.radio(
            "How do you want to choose states?",
            options=["Use default list in code", "Enter custom list (comma-separated)"],
            horizontal=True,
        )
        if states_mode == "Use default list in code":
            states_of_interest = DEFAULT_STATES[:]
        else:
            states_input = st.text_area(
                "Enter states separated by commas (e.g. Indiana, Florida, Maryland)",
                value="",
                height=80,
            )
            states_of_interest = parse_str_list(states_input)

    st.write("States to export:")
    st.write(states_of_interest)

# -----------------------------
# Step 3: Fetch availability file listing for date (same call)
# -----------------------------
st.subheader("3) Choose technology types and filters")

fetch_listing = st.button("Fetch availability listing for selected date", disabled=not bool(as_of_date))

if fetch_listing:
    r1 = requests.get(f"{base_url}/downloads/listAvailabilityData/{as_of_date}", headers=headers)
    if r1.status_code == 200:
        data = r1.json()["data"]

        # Ensure provider_id/provider_name always string (preserve your mechanism)
        for row in data:
            row["provider_id"] = str(row["provider_id"]) if row["provider_id"] is not None else "Blank"
            row["provider_name"] = str(row["provider_name"]) if row["provider_name"] is not None else "Blank"
        for row in data:
            if "speed_tier" in row:
                row["speed_tier"] = str(row["speed_tier"])

        df1 = pl.DataFrame(data)

        # Fixed Broadband, category == State (same filtering)
        df1_fixed_broadband = df1.filter(df1["technology_type"] == "Fixed Broadband")
        df1_fixed_broadband = df1_fixed_broadband.filter(df1_fixed_broadband["category"] == "State")

        tech_list = df1_fixed_broadband["technology_code_desc"].unique().drop_nulls().sort()
        tech_list_py = tech_list.to_list()

        st.session_state["df1_fixed_broadband"] = df1_fixed_broadband
        st.session_state["tech_list"] = tech_list_py

        st.success("Availability listing loaded.")
        st.write("Available technology types:")
        st.dataframe(pl.DataFrame({"technology_code_desc": tech_list}).to_pandas())
    else:
        st.error("Unable to fetch the data.")
        st.code(r1.text)

tech_list_py = st.session_state.get("tech_list", [])
df1_fixed_broadband = st.session_state.get("df1_fixed_broadband", None)

tech_of_interest_list = st.multiselect(
    "Select technology types of interest",
    options=tech_list_py,
    default=tech_list_py[:1] if tech_list_py else [],
    disabled=not bool(tech_list_py),
)

# Mirrors y/n inputs
resi_choice = st.radio("Residential-only service?", options=["y", "n"], horizontal=True, index=1)

include_location_providers = st.radio(
    "Specify providers to define location IDs?",
    options=["y", "n"],
    horizontal=True,
    index=1,
)
location_provider_ids = []
if include_location_providers == "y":
    location_provider_ids = parse_int_list(
        st.text_input("Provider IDs to define location IDs (comma-separated)", value="")
    )

include_provider_subset = st.radio(
    "Define a subset of a provider's footprint based on technology? (e.g., if you want to analyze Verizon's DSL + Fiber footprint only. If you leave this blank, the code will include Cable, Copper, and Fiber, so best practice is to define this.",
    options=["y", "n"],
    horizontal=True,
    index=1,
)
provider_subset_tech = None
if include_provider_subset == "y":
    provider_subset_tech = st.multiselect(
        "Subset technologies to define provider footprint",
        options=tech_list_py,
        default=[],
        disabled=not bool(tech_list_py),
    )

exclude_providers = st.radio("Exclude any providers?", options=["y", "n"], horizontal=True, index=1)
excluded_provider_ids = []
if exclude_providers == "y":
    excluded_provider_ids = parse_int_list(
        st.text_input("Provider IDs to exclude (comma-separated)", value="")
    )

group_on_speed = st.radio("Group on speed tier as well?", options=["y", "n"], horizontal=True, index=1)
group_on_technology = st.radio(
    "Group purely by technology (count unique locations per CB for technology)?",
    options=["y", "n"],
    horizontal=True,
    index=1,
)

# Upload CB list (replaces glob selection; function remains: get list of block_geoid)
cb_choice = st.radio("Upload your own list of CB geoids?", options=["y", "n"], horizontal=True, index=1)
user_cb_geoids = None
if cb_choice == "y":
    uploaded_cb = st.file_uploader("Upload CSV containing a 'block_geoid' column", type=["csv"])
    if uploaded_cb is not None:
        user_cb_geoids = pl.read_csv(uploaded_cb)["block_geoid"].to_list()
        st.success(f"Loaded {len(user_cb_geoids)} block_geoids from uploaded file.")

st.subheader("4) Optional rollup by geography")

rollup_choice = st.radio(
    "Do you want to roll up block-level GEOIDs to a higher geography?",
    options=["n", "y"],
    horizontal=True,
    index=0,
)
rollup_level = None
if rollup_choice == "y":
    rollup_level = st.selectbox(
        "Select rollup geography",
        options=["State", "County", "Tract", "CBG"],
        index=1,
    )

# -----------------------------
# Step 4: Run (same loops & logic; just add progress + download)
# -----------------------------
st.subheader("5) Run export")

run_export = st.button(
    "Run export",
    disabled=(
        (df1_fixed_broadband is None)
        or (not tech_of_interest_list)
        or (not states_of_interest)
        or (not as_of_date)
    ),
)

if run_export:
    dfs_dict = {}

    progress = st.progress(0)
    status = st.empty()

    total_states = len(states_of_interest)
    for s_idx, state in enumerate(states_of_interest, start=1):
        combined_raw_df_for_state = pl.DataFrame()

        for tech_of_interest in tech_of_interest_list:
            # Filter by state + tech (same)
            df1_filtered = df1_fixed_broadband.filter(df1_fixed_broadband["state_name"] == state)
            df1_filtered = df1_filtered.filter(df1_filtered["technology_code_desc"] == tech_of_interest)

            for row in df1_filtered.iter_rows(named=True):
                file_id = row["file_id"]
                r2 = requests.get(
                    f"{base_url}/downloads/downloadFile/availability/{file_id}/1",
                    headers=headers,
                )
                if r2.status_code == 200:
                    status.write(f"Downloading data for {state} | file_id={file_id} ({tech_of_interest})...")
                    with zipfile.ZipFile(io.BytesIO(r2.content)) as z:
                        with z.open(z.namelist()[0]) as f:
                            df_raw = pl.read_csv(f)

                    combined_raw_df_for_state = combined_raw_df_for_state.vstack(df_raw)
                else:
                    st.warning(f"Could not download data for file_id = {file_id} ({r2.status_code}).")
                    st.code(r2.text)

            # Re-download a copy of the location data for the provider of interest's tech footprint
            if include_location_providers == "y":
                combined_provider_subset_df_for_state = pl.DataFrame()
                if include_provider_subset == "y":
                    tech_list = provider_subset_tech
                else:
                    tech_list = ["Cable", "Copper", "Fiber to the Premises"]
                for tech_of_interest in tech_list:
                    df1_filtered = df1_fixed_broadband.filter(df1_fixed_broadband['state_name'] == state)
                    df1_filtered = df1_filtered.filter(df1_filtered['technology_code_desc'] == tech_of_interest)

                    for row in df1_filtered.iter_rows(named=True):
                        file_id = row['file_id']
                        r2 = requests.get(f'{base_url}/downloads/downloadFile/availability/{file_id}/1', headers=headers)
                        if r2.status_code == 200:
                            # Unzip the content
                            print(f"Re-downloading data for file_id = {file_id} ({tech_of_interest}) for provider subset...")
                            with zipfile.ZipFile(io.BytesIO(r2.content)) as z:
                                with z.open(z.namelist()[0]) as f:
                                    df_raw_subset = pl.read_csv(f)

                            # Combine raw data for all technologies before applying filters
                            combined_provider_subset_df_for_state = combined_provider_subset_df_for_state.vstack(df_raw_subset)
                        else:
                            print(f"Could not download data for file_id = {file_id} ({r2.status_code}).")
                            print(r2.text)

        # Apply filters (same)
        if resi_choice == "y":
            combined_raw_df_for_state = combined_raw_df_for_state.filter(
                combined_raw_df_for_state["business_residential_code"] != "B"
            )
            combined_provider_subset_df_for_state = combined_provider_subset_df_for_state.filter(combined_provider_subset_df_for_state["business_residential_code"] != "B")

        # Apply location provider logic (same)
        if location_provider_ids:
            loc_ids = combined_provider_subset_df_for_state.filter(
                combined_provider_subset_df_for_state['provider_id'].is_in(location_provider_ids)
            )['location_id'].unique()

            combined_raw_df_for_state = combined_raw_df_for_state.filter(
                combined_raw_df_for_state["location_id"].is_in(loc_ids)
            )

        # Exclude providers (same)
        if excluded_provider_ids:
            combined_raw_df_for_state = combined_raw_df_for_state.filter(
                ~combined_raw_df_for_state["provider_id"].is_in(excluded_provider_ids)
            )

        # Group by speed tier if selected (same)
        if group_on_speed == "y":
            combined_raw_df_for_state = combined_raw_df_for_state.group_by(
                ["provider_id", "block_geoid", "location_id"]
            ).agg(pl.col("max_advertised_download_speed").max())

        # Grouping options (same branching)
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

        # Filter by uploaded CBs (same)
        if user_cb_geoids is not None:
            combined_raw_df_for_state = combined_raw_df_for_state.filter(
                combined_raw_df_for_state["block_geoid"].is_in(user_cb_geoids)
            )

        # -----------------------------
        # If rollup requested, roll up *within each state before vstack* to keep the merged file from getting huge.
        # Then pivot once at the very end (after all states are stacked).
        # -----------------------------
        if rollup_choice == "y":
            value_col = "location_id"  # created by your n_unique aggregation
            if "block_geoid" not in combined_raw_df_for_state.columns:
                st.error('Cannot roll up because "block_geoid" column is missing in the state output.')
                st.stop()
            if value_col not in combined_raw_df_for_state.columns:
                st.error('Cannot roll up because the expected count column "location_id" is missing in the state output.')
                st.stop()

            prefix_len = _block_geoid_prefix_len(rollup_level)
            combined_raw_df_for_state = combined_raw_df_for_state.with_columns(
                pl.col("block_geoid").cast(pl.Utf8).str.slice(0, prefix_len).alias("block_geoid")
            )

            # Pivot is potentially memory intensive; we keep data long while rolling up.
            # Only pivot later if needed.
            pivot_needed = (group_on_technology == "n") or (group_on_speed == "y")

            # If provider_id and/or speed exist, pre-aggregate long-form per state to reduce rows.
            if pivot_needed and ("provider_id" in combined_raw_df_for_state.columns):
                # provider + speed case: create combined key
                if (group_on_technology == "n") and (group_on_speed == "y") and ("max_advertised_download_speed" in combined_raw_df_for_state.columns):
                    combined_raw_df_for_state = combined_raw_df_for_state.with_columns(
                        (
                            pl.col("provider_id").cast(pl.Utf8)
                            + pl.lit("_")
                            + pl.col("max_advertised_download_speed").cast(pl.Utf8)
                        ).alias("provider_speed")
                    )
                    combined_raw_df_for_state = combined_raw_df_for_state.group_by(["block_geoid", "provider_speed"]).agg(
                        pl.col(value_col).sum()
                    )
                else:
                    # provider-only case
                    combined_raw_df_for_state = combined_raw_df_for_state.group_by(["block_geoid", "provider_id"]).agg(
                        pl.col(value_col).sum()
                    )
            else:
                # no pivot required (or no provider info): just roll up by geography
                combined_raw_df_for_state = combined_raw_df_for_state.group_by(["block_geoid"]).agg(
                    pl.col(value_col).sum()
                )

        dfs_dict[state] = combined_raw_df_for_state
        status.write(f"Data for {state} processed.")

        progress.progress(int((s_idx / total_states) * 100))

    # Merge all states (same)
    df_merged = pl.DataFrame()
    for state_name in dfs_dict:
        df_merged = df_merged.vstack(dfs_dict[state_name])

    # -----------------------------
    # If rollup requested, we've already rolled up per-state before stacking.
    # Now (at the very end), pivot everything if needed.
    # -----------------------------

    if rollup_choice == "y":
        value_col = "location_id"

        # Determine whether we should pivot based on the user's grouping choices.
        pivot_needed = (group_on_technology == "n") or (group_on_speed == "y")

        if pivot_needed:
            # provider+speed pivots on provider_speed if present; else provider_id
            if "provider_speed" in df_merged.columns:
                df_merged = df_merged.pivot(
                    values=value_col,
                    index="block_geoid",
                    columns="provider_speed",
                    aggregate_function="sum",
                )
            elif "provider_id" in df_merged.columns:
                df_merged = df_merged.pivot(
                    values=value_col,
                    index="block_geoid",
                    columns="provider_id",
                    aggregate_function="sum",
                )
            else:
                # Nothing to pivot on; keep as-is
                pass

    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    tech_names_combined = "_".join([tech.replace(" ", "") for tech in tech_of_interest_list])
    out_name = f"{tech_names_combined}_{current_datetime}.csv"

    # In Streamlit, provide a download instead of relying on local disk write.
    # This preserves output contents + filename convention.
    csv_bytes = df_merged.write_csv().encode("utf-8")

    st.success(f"Data output ready: {out_name}")
    st.write("Preview (first 200 rows):")
    st.dataframe(df_merged.head(200).to_pandas())

    st.download_button(
        label="Download CSV",
        data=csv_bytes,
        file_name=out_name,
        mime="text/csv",
    )

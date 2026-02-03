# app.py
# Streamlit version of the provided script.
#
# Prereqs:
#   pip install streamlit requests polars
# Run:
#   streamlit run app.py
#
# Secrets (recommended):
#   Create .streamlit/secrets.toml with:
#     username = "chris.chen1@parthenon.ey.com"
#     hash_value = "..."
#
# Notes:
# - This can pull very large ZIP/CSV files from the FCC BDC API. Start with 1–2 states and 1 tech.
# - Streamlit Community Cloud requires that the FCC endpoint is reachable from the app environment.

import io
import zipfile
from datetime import datetime
from typing import Iterable, List, Optional

import polars as pl
import requests
import streamlit as st

# -----------------------------
# Configuration
# -----------------------------
pl.Config.set_tbl_rows(50)

BASE_URL = "https://bdc.fcc.gov/api/public/map"  # no trailing slash

DEFAULT_STATES = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "District of Columbia",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Puerto Rico",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
    "Washington",
]


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


def _normalize_int_list(text: str) -> List[int]:
    if not text.strip():
        return []
    out: List[int] = []
    for tok in text.split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(int(tok))
    return out


@st.cache_data(show_spinner=False)
def fetch_as_of_dates(headers: dict) -> pl.DataFrame:
    r = requests.get(f"{BASE_URL}/listAsOfDates", headers=headers, timeout=60)
    r.raise_for_status()
    data = r.json().get("data", [])
    df = pl.DataFrame(data)
    return df


@st.cache_data(show_spinner=False)
def fetch_availability_manifest(headers: dict, as_of_date: str) -> pl.DataFrame:
    r = requests.get(f"{BASE_URL}/downloads/listAvailabilityData/{as_of_date}", headers=headers, timeout=60)
    r.raise_for_status()
    data = r.json().get("data", [])

    # Ensure some fields have stable types
    for row in data:
        row["provider_id"] = str(row.get("provider_id")) if row.get("provider_id") is not None else "Blank"
        row["provider_name"] = str(row.get("provider_name")) if row.get("provider_name") is not None else "Blank"
        if "speed_tier" in row:
            row["speed_tier"] = str(row.get("speed_tier"))

    return pl.DataFrame(data)


@st.cache_data(show_spinner=False)
def download_and_read_zip_csv(headers: dict, file_id: str) -> pl.DataFrame:
    """Download a ZIP from FCC and read the first file inside as CSV with Polars."""
    r = requests.get(
        f"{BASE_URL}/downloads/downloadFile/availability/{file_id}/1",
        headers=headers,
        timeout=300,
    )
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        first = z.namelist()[0]
        with z.open(first) as f:
            return pl.read_csv(f)


def maybe_filter_residential(df: pl.DataFrame, residential_only: bool) -> pl.DataFrame:
    if not residential_only:
        return df
    if "business_residential_code" not in df.columns:
        return df
    return df.filter(pl.col("business_residential_code") != "B")


def apply_location_provider_logic(
    df: pl.DataFrame,
    location_provider_ids: List[int],
    provider_subset_tech: Optional[List[str]],
) -> pl.DataFrame:
    if not location_provider_ids:
        return df

    if "provider_id" not in df.columns or "location_id" not in df.columns:
        return df

    # provider_id in raw files is often numeric; be robust
    provider_id_col = pl.col("provider_id")

    # Base set of location IDs covered by the chosen provider(s)
    loc_ids = (
        df.filter(provider_id_col.cast(pl.Int64, strict=False).is_in(location_provider_ids))
        .select(pl.col("location_id"))
        .unique()
    )

    # Optional: subset those loc_ids further by technology
    if provider_subset_tech and "technology_code_desc" in df.columns:
        loc_ids = (
            df.filter(
                pl.col("location_id").is_in(loc_ids["location_id"])
                & pl.col("technology_code_desc").is_in(provider_subset_tech)
            )
            .select(pl.col("location_id"))
            .unique()
        )

    return df.filter(pl.col("location_id").is_in(loc_ids["location_id"]))


def exclude_providers(df: pl.DataFrame, excluded_provider_ids: List[int]) -> pl.DataFrame:
    if not excluded_provider_ids:
        return df
    if "provider_id" not in df.columns:
        return df
    return df.filter(~pl.col("provider_id").cast(pl.Int64, strict=False).is_in(excluded_provider_ids))


def group_data(
    df: pl.DataFrame,
    group_on_speed: bool,
    group_on_technology: bool,
) -> pl.DataFrame:
    """Replicates the grouping logic from the original script."""

    # First: if grouping on speed, collapse to max speed per provider/block/location
    if group_on_speed and {"provider_id", "block_geoid", "location_id", "max_advertised_download_speed"}.issubset(
        set(df.columns)
    ):
        df = df.group_by(["provider_id", "block_geoid", "location_id"]).agg(
            pl.col("max_advertised_download_speed").max()
        )

    # Then: produce final aggregation
    if group_on_technology:
        # Count unique locations per block
        if {"block_geoid", "location_id"}.issubset(set(df.columns)):
            return df.group_by(["block_geoid"]).agg(pl.col("location_id").n_unique().alias("n_unique_locations"))
        return df

    if group_on_speed:
        # provider + block + speed
        if {"provider_id", "block_geoid", "max_advertised_download_speed", "location_id"}.issubset(set(df.columns)):
            return df.group_by(["provider_id", "block_geoid", "max_advertised_download_speed"]).agg(
                pl.col("location_id").n_unique().alias("n_unique_locations")
            )
        return df

    # Default: provider + block
    if {"provider_id", "block_geoid", "location_id"}.issubset(set(df.columns)):
        return df.group_by(["provider_id", "block_geoid"]).agg(
            pl.col("location_id").n_unique().alias("n_unique_locations")
        )

    return df


# -----------------------------
# UI
# -----------------------------

st.set_page_config(page_title="FCC BDC Availability Downloader", layout="wide")

st.title("FCC BDC Availability Downloader (Streamlit)")

headers = _headers_from_secrets()

with st.expander("What this app does", expanded=False):
    st.markdown(
        """
This is a Streamlit UI wrapper around your script:

1. Lists available **as-of** dates from the FCC BDC public map API.
2. For a chosen date, lists the availability download manifest.
3. Lets you pick states + fixed-broadband technologies.
4. Downloads the underlying ZIP(s), combines them, applies your filters, aggregates, and returns a merged dataset.

**Tip:** Start small (1 state, 1 technology). These files can be huge.
"""
    )

# --- Fetch dates
if headers.get("username") and headers.get("hash_value"):
    with st.spinner("Fetching available as-of dates..."):
        try:
            df_dates = fetch_as_of_dates(headers)
        except Exception as e:
            st.error(f"Failed to fetch as-of dates: {e}")
            st.stop()

    df_avail_dates = df_dates.filter(pl.col("data_type") == "availability") if "data_type" in df_dates.columns else df_dates

    # Choose date
    asof_options: List[str] = []
    if "as_of_date" in df_avail_dates.columns:
        asof_options = (
            df_avail_dates.select(pl.col("as_of_date").cast(pl.Utf8)).unique().sort("as_of_date")["as_of_date"].to_list()
        )

    col1, col2 = st.columns([2, 3])
    with col1:
        as_of_date = st.selectbox("As-of date", options=asof_options or [""], index=(len(asof_options) - 1) if asof_options else 0)
    with col2:
        st.caption("If this is blank, check your API headers in the sidebar.")

    st.subheader("Filters")

    # States
    states_of_interest = st.multiselect("States", options=sorted(DEFAULT_STATES), default=["Kansas"])

    # Download manifest
    if as_of_date:
        with st.spinner("Fetching availability download manifest..."):
            try:
                df_manifest = fetch_availability_manifest(headers, as_of_date)
            except Exception as e:
                st.error(f"Failed to fetch availability manifest for {as_of_date}: {e}")
                st.stop()

        # Fixed Broadband + State-level (matches original)
        df_fixed = df_manifest
        if {"technology_type", "category"}.issubset(set(df_fixed.columns)):
            df_fixed = df_fixed.filter((pl.col("technology_type") == "Fixed Broadband") & (pl.col("category") == "State"))

        tech_list: List[str] = []
        if "technology_code_desc" in df_fixed.columns:
            tech_list = df_fixed.select(pl.col("technology_code_desc")).unique().drop_nulls().sort("technology_code_desc")[
                "technology_code_desc"
            ].to_list()

        tech_of_interest_list = st.multiselect("Fixed broadband technology type(s)", options=tech_list, default=tech_list[:1])

        residential_only = st.checkbox("Residential-only service (exclude business 'B')", value=False)

        st.markdown("---")
        st.subheader("Provider-based location filtering")

        include_location_providers = st.checkbox("Specify providers to define location IDs", value=False)
        location_provider_ids: List[int] = []
        if include_location_providers:
            location_provider_ids = _normalize_int_list(
                st.text_input("Provider IDs to define location IDs (comma-separated)", value="")
            )

        include_provider_subset = st.checkbox("Define a subset of those providers' footprint based on technology", value=False)
        provider_subset_tech: Optional[List[str]] = None
        if include_provider_subset:
            provider_subset_tech = st.multiselect(
                "Subset technology(ies) for provider footprint",
                options=tech_list,
                default=tech_of_interest_list[:1] if tech_of_interest_list else None,
            )

        exclude_providers_flag = st.checkbox("Exclude providers", value=False)
        excluded_provider_ids: List[int] = []
        if exclude_providers_flag:
            excluded_provider_ids = _normalize_int_list(st.text_input("Provider IDs to exclude (comma-separated)", value=""))

        st.markdown("---")
        st.subheader("Grouping")
        group_on_speed = st.checkbox("Group on speed tier", value=False)
        group_on_technology = st.checkbox(
            "Group purely by technology (count unique locations per block, no provider split)", value=False
        )

        st.markdown("---")
        st.subheader("Optional: filter to your own CB GEOIDs")
        cb_file = st.file_uploader("Upload CSV with a 'block_geoid' column", type=["csv"], accept_multiple_files=False)
        user_cb_geoids: Optional[List[str]] = None
        if cb_file is not None:
            try:
                cb_df = pl.read_csv(cb_file)
                if "block_geoid" not in cb_df.columns:
                    st.error("Your CSV must contain a 'block_geoid' column.")
                else:
                    user_cb_geoids = cb_df["block_geoid"].cast(pl.Utf8).to_list()
                    st.caption(f"Loaded {len(user_cb_geoids):,} block GEOIDs")
            except Exception as e:
                st.error(f"Could not read uploaded CSV: {e}")

        st.markdown("---")

        run = st.button("Run download + process", type="primary", disabled=not (states_of_interest and tech_of_interest_list))

        if run:
            if not states_of_interest:
                st.error("Select at least one state.")
                st.stop()
            if not tech_of_interest_list:
                st.error("Select at least one technology.")
                st.stop()

            # Process
            progress = st.progress(0)
            status = st.empty()

            dfs: List[pl.DataFrame] = []
            total_steps = max(1, len(states_of_interest) * len(tech_of_interest_list))
            step = 0

            for state in states_of_interest:
                combined_raw = pl.DataFrame()

                for tech in tech_of_interest_list:
                    step += 1
                    progress.progress(min(1.0, step / total_steps))
                    status.write(f"State: **{state}** | Technology: **{tech}**")

                    # Find file ids for this state + tech
                    if {"state_name", "technology_code_desc", "file_id"}.issubset(set(df_fixed.columns)):
                        df_sel = df_fixed.filter((pl.col("state_name") == state) & (pl.col("technology_code_desc") == tech))
                    else:
                        st.error("Manifest is missing required columns (state_name, technology_code_desc, file_id).")
                        st.stop()

                    if df_sel.height == 0:
                        st.warning(f"No files found for {state} / {tech}")
                        continue

                    # Download each file_id and append
                    for row in df_sel.iter_rows(named=True):
                        file_id = str(row.get("file_id"))
                        try:
                            df_raw = download_and_read_zip_csv(headers, file_id)
                            # Keep the tech description around if present in raw (sometimes it is; sometimes not)
                            if "technology_code_desc" not in df_raw.columns:
                                df_raw = df_raw.with_columns(pl.lit(tech).alias("technology_code_desc"))
                            combined_raw = df_raw if combined_raw.is_empty() else combined_raw.vstack(df_raw)
                        except Exception as e:
                            st.error(f"Failed downloading file_id={file_id} ({state} / {tech}): {e}")
                            st.stop()

                # Apply filters & grouping for this state
                if combined_raw.is_empty():
                    continue

                combined_raw = maybe_filter_residential(combined_raw, residential_only)
                combined_raw = apply_location_provider_logic(combined_raw, location_provider_ids, provider_subset_tech)
                combined_raw = exclude_providers(combined_raw, excluded_provider_ids)

                combined_grouped = group_data(combined_raw, group_on_speed, group_on_technology)

                if user_cb_geoids is not None and "block_geoid" in combined_grouped.columns:
                    combined_grouped = combined_grouped.filter(pl.col("block_geoid").cast(pl.Utf8).is_in(user_cb_geoids))

                # Keep state label for traceability
                combined_grouped = combined_grouped.with_columns(pl.lit(state).alias("state_name"))
                dfs.append(combined_grouped)

            if not dfs:
                st.warning("No data returned for your selections.")
                st.stop()

            df_merged = pl.concat(dfs, how="vertical_relaxed")

            st.success(f"Done. Rows: {df_merged.height:,} | Cols: {len(df_merged.columns)}")

            # Display
            st.subheader("Preview")
            st.dataframe(df_merged.head(200).to_pandas(), use_container_width=True)

            # Download
            current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
            tech_names_combined = "_".join([t.replace(" ", "") for t in tech_of_interest_list])
            filename = f"{tech_names_combined}_{current_datetime}.csv"

            csv_bytes = df_merged.write_csv().encode("utf-8")
            st.download_button(
                label=f"Download CSV ({filename})",
                data=csv_bytes,
                file_name=filename,
                mime="text/csv",
            )

            st.caption("If you need the exact original-file output behavior, you can also write to disk when running locally.")

    # Optional: show available dates table
    with st.expander("Show available 'availability' as-of dates table"):
        try:
            st.dataframe(df_avail_dates.to_pandas(), use_container_width=True)
        except Exception:
            st.write(df_avail_dates)

else:
    st.info("Enter API headers in the sidebar to begin.")

import requests
import os
import json
from datetime import datetime, timedelta, timezone, date
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

# --- Load Environment Variables ---
# Load variables from .env file into environment variables
load_dotenv()

# --- Configuration (from .env file) ---
API_TOKEN = os.getenv("DBT_CLOUD_API_TOKEN")
ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID")  # Must be a string containing only numbers
BASE_URL = os.getenv("DBT_CLOUD_BASE_URL", "https://au.dbt.com")  # Default to au.dbt.com if not specified

# print(f"DEBUG: Value loaded for ACCOUNT_ID: {ACCOUNT_ID}") # DEBUG LINE REMOVED

# --- Basic Validation ---
if not API_TOKEN or API_TOKEN == "YOUR_DBT_CLOUD_API_TOKEN_HERE":
    st.error("API_TOKEN not set. Please replace the placeholder in the script.")
    st.stop()
if not ACCOUNT_ID or ACCOUNT_ID == "YOUR_NUMERIC_ACCOUNT_ID_HERE":
     st.error("ACCOUNT_ID not set. Please replace the placeholder in the script.")
     st.stop()
if not ACCOUNT_ID.isdigit():
     st.error(f"ACCOUNT_ID '{ACCOUNT_ID}' is not valid. It must be a string containing only numbers.")
     st.stop()
if not BASE_URL:
    st.error("BASE_URL not set or empty in the script.")
    st.stop()


print(f"Using Account ID: {ACCOUNT_ID}")
print(f"Using Base URL: {BASE_URL}")
print("API Token is set (not printing value).")

# --- Headers for Authentication ---
AUTH_HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json", # Standard for JSON API requests
    "Accept": "application/json",       # Explicitly accept JSON responses
}

print("Script setup complete. Ready for next steps.")

# --- Helper for making API requests ---
def make_dbt_cloud_request(endpoint, params=None):
    """Makes a GET request to the dbt Cloud API, handling basic errors and returning JSON.

    Args:
        endpoint (str): The API endpoint path (e.g., '/api/v2/accounts/123/runs/').
        params (dict, optional): Dictionary of query parameters for the request.

    Returns:
        dict: The JSON response data if successful, None otherwise.
    """
    if not endpoint.startswith('/'):
        endpoint = f'/{endpoint}' # Ensure leading slash

    url = f"{BASE_URL}{endpoint}"
    # Use st.spinner for visual feedback in Streamlit later, but keep print for now
    # print(f"Making API request to: {url} with params: {params}")

    try:
        response = requests.get(
            url,
            headers=AUTH_HEADERS,
            params=params,
            timeout=60 # Add a timeout (in seconds)
        )
        # Raise an HTTPError exception for bad responses (4xx or 5xx)
        response.raise_for_status()

        # Attempt to parse JSON
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        st.error(f"HTTP error occurred: {http_err} - Status Code: {http_err.response.status_code}")
        try:
            # Try to print the JSON error response from dbt Cloud if available
            st.error(f"Response Body: {http_err.response.json()}")
        except json.JSONDecodeError:
            # Otherwise print the raw text
            st.error(f"Response Body: {http_err.response.text}")
        return None # Indicate failure
    except requests.exceptions.RequestException as req_err:
        # Catch other request errors (connection, timeout, etc.)
        st.error(f"Error making API request to {url}: {req_err}")
        return None # Indicate failure
    except json.JSONDecodeError as json_err:
        # Catch errors if the response isn't valid JSON (unexpected)
        st.error(f"Error decoding JSON response from {url}: {json_err}")
        st.error(f"Response Text: {response.text if 'response' in locals() else 'N/A'}")
        return None # Indicate failure

# --- Helper function to fetch ALL items from a paginated endpoint ---
@st.cache_data(ttl=3600) # Cache for 1 hour
def get_all_items(endpoint, limit_per_page=100):
    """Fetches all items from a paginated v2 list endpoint."""
    all_items = []
    offset = 0
    # st.write(f"Fetching all items from {endpoint}...") # Debug message
    page = 1
    while True:
        # print(f"Fetching page {page} for {endpoint} (Offset: {offset})") # Debug
        params = {"limit": limit_per_page, "offset": offset}
        response_data = make_dbt_cloud_request(endpoint, params=params)
        if response_data is None:
            st.error(f"Failed to fetch data from {endpoint} during pagination (page {page}).")
            return None # Indicate failure

        items_page = response_data.get('data', [])
        if not items_page:
            # print(f"No more items found for {endpoint} after page {page-1}.") # Debug
            break # No more items

        all_items.extend(items_page)

        # Check pagination metadata for total count (more robust stop condition)
        total_count = response_data.get('extra', {}).get('pagination', {}).get('total_count', -1)
        current_count_fetched = offset + len(items_page)

        if len(items_page) < limit_per_page:
            # print(f"Fetched less than limit on page {page} for {endpoint}. Assuming end.") # Debug
            break # Fetched less than limit, must be the last page
        if total_count != -1 and current_count_fetched >= total_count:
            # print(f"Fetched total count ({total_count}) for {endpoint}.") # Debug
             break # Fetched all items according to API

        offset += len(items_page)
        page += 1
    # st.write(f"Finished fetching {len(all_items)} items from {endpoint}.") # Debug message
    return all_items

# --- Specific Fetch Functions using the helper ---
@st.cache_data(ttl=3600)
def get_all_jobs():
    """Fetches all job definitions for the account."""
    endpoint = f"/api/v2/accounts/{ACCOUNT_ID}/jobs/"
    return get_all_items(endpoint)

@st.cache_data(ttl=3600)
def get_all_projects():
    """Fetches all projects for the account."""
    endpoint = f"/api/v2/accounts/{ACCOUNT_ID}/projects/"
    return get_all_items(endpoint)

@st.cache_data(ttl=3600)
def get_all_environments():
    """Fetches all environments for the account."""
    endpoint = f"/api/v2/accounts/{ACCOUNT_ID}/environments/"
    return get_all_items(endpoint)

# --- Function to fetch job runs for a specific day ---
# Renamed and modified to take a date argument
@st.cache_data(ttl=600) # Cache results for 10 minutes
def get_runs_for_day(target_date: date, limit_per_page=100, max_pages_to_check=20):
    """Retrieves dbt Cloud job runs created on target_date (UTC), filtering client-side.

    Fetches runs ordered by newest first and stops when runs before target_date are found.

    Args:
        target_date (date): The date to fetch runs for.
        limit_per_page (int): How many runs to fetch per API request.
        max_pages_to_check (int): Safety limit on the number of pages to fetch.

    Returns:
        list: A list of run dictionaries created on target_date if successful, None otherwise.
    """
    all_runs_today = []
    offset = 0
    endpoint = f"/api/v2/accounts/{ACCOUNT_ID}/runs/"

    # --- Calculate Start and End of Target Day (UTC) ---
    day_start_utc = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
    day_end_utc = day_start_utc + timedelta(days=1)

    st.write(f"Fetching recent runs for account {ACCOUNT_ID} and filtering for {target_date.isoformat()} UTC...")

    for page_num in range(max_pages_to_check):
        st.write(f"Fetching page {page_num + 1}/{max_pages_to_check}... (Offset: {offset})")
        params = {
            "limit": limit_per_page,
            "offset": offset,
            "order_by": "-created_at",
        }
        response_data = make_dbt_cloud_request(endpoint, params=params)

        if response_data is None:
            st.warning(f"API request failed during pagination. Returning potentially incomplete list of {len(all_runs_today)} runs found so far.")
            return all_runs_today if all_runs_today else [] # Return list or empty list

        runs_page = response_data.get('data', [])
        if not runs_page:
            st.write("No more runs found in API history.")
            break

        st.write(f"Fetched {len(runs_page)} runs from API page.")
        found_run_before_day = False
        for run in runs_page:
            run_timestamp_str = run.get('created_at')
            if not run_timestamp_str: continue
            try:
                run_time_utc = datetime.fromisoformat(run_timestamp_str.replace('Z', '+00:00'))
                if run_time_utc.tzinfo is None: run_time_utc = run_time_utc.replace(tzinfo=timezone.utc)
            except ValueError: continue

            if day_start_utc <= run_time_utc < day_end_utc:
                all_runs_today.append(run)
            elif run_time_utc < day_start_utc:
                st.write(f"Found run {run.get('id')} ({run_timestamp_str}) before target date. Stopping pagination.")
                found_run_before_day = True
                break
        
        if found_run_before_day: break
        if len(runs_page) < limit_per_page: break
        total_count = response_data.get('extra', {}).get('pagination', {}).get('total_count', -1)
        current_count_checked = offset + len(runs_page)
        if total_count != -1 and current_count_checked >= total_count: break
        offset += len(runs_page)
    else:
         st.warning(f"Stopped fetching after checking {max_pages_to_check} pages. Results might be incomplete.")

    st.write(f"Total runs found for {target_date.isoformat()} (after client-side filtering): {len(all_runs_today)}")
    return all_runs_today

# --- Streamlit App UI ---
st.set_page_config(layout="wide", page_title="dbt Cloud Run Monitor")
st.title("dbt Cloud Job Run Monitor")
st.markdown(f"Account ID: `{ACCOUNT_ID}` | Base URL: `{BASE_URL}`")

# --- Date Selection --- 
today = date.today()
# Use a list/tuple for value to enable range selection
selected_date_range = st.date_input(
    "Select Date Range to View Runs", 
    value=(today, today), # Default to today
    max_value=today # Prevent selecting future dates
    )

# Ensure we have a start and end date (date_input returns tuple for range)
start_date = None
end_date = None
if isinstance(selected_date_range, (list, tuple)) and len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
    if start_date > end_date:
        st.warning("Start date cannot be after end date. Using end date for both.")
        start_date = end_date # Or swap them
elif isinstance(selected_date_range, date): # Handle case where user might force single date
     start_date = selected_date_range
     end_date = selected_date_range
else:
    st.error("Invalid date range selected.")
    st.stop() # Stop execution if date range is invalid

# Use columns for layout
col1, col2 = st.columns([1, 3])

with col1:
    # Initialize session state for dataframe if it doesn't exist
    if 'runs_df' not in st.session_state:
        st.session_state['runs_df'] = pd.DataFrame()

    if st.button("Fetch Runs", type="primary"):
        # Clear previous results before fetching new ones
        st.session_state['runs_df'] = pd.DataFrame()
        all_runs_in_range = [] # List to hold results from all days
        
        # Calculate date range
        if start_date and end_date:
             date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
             spinner_text = f"Fetching data from {start_date.isoformat()} to {end_date.isoformat()}..."
        else: # Should not happen due to checks above, but fallback
             date_list = []
             spinner_text = "Fetching data..."

        with st.spinner(spinner_text):
            # 1. Fetch Runs for each day in the range
            total_runs_fetched = 0
            fetch_failed = False
            for current_date in date_list:
                st.write(f"--- Fetching for {current_date.isoformat()} ---")
                runs_for_current_day = get_runs_for_day(current_date) # Use existing cached function
                if runs_for_current_day is None:
                    # If get_runs_for_day returns None, it indicates an API error during fetch
                    st.error(f"Failed to fetch runs for {current_date.isoformat()}. Stopping.")
                    fetch_failed = True
                    break # Stop fetching if one day fails
                elif runs_for_current_day: # Check if list is not empty
                    all_runs_in_range.extend(runs_for_current_day)
                    total_runs_fetched += len(runs_for_current_day)
                    st.write(f"-> Found {len(runs_for_current_day)} runs.")
                else:
                     st.write(f"-> Found 0 runs.")

            if fetch_failed:
                 st.stop() # Stop if any day failed
            
            if not all_runs_in_range:
                st.warning("No runs found for the selected date range.")
                st.session_state['runs_df'] = pd.DataFrame() # Ensure it's an empty DF
                st.stop()

            st.write(f"--- Total runs found in range: {len(all_runs_in_range)} ---")
            runs_df = pd.DataFrame(all_runs_in_range)
            runs_df['created_at'] = pd.to_datetime(runs_df['created_at'])

            # 2. Fetch related definitions (Jobs, Projects, Environments) - Only needed once
            st.write("Fetching related definitions (Jobs, Projects, Environments)...")
            all_jobs = get_all_jobs()
            all_projects = get_all_projects()
            all_environments = get_all_environments()

            if all_jobs is None or all_projects is None or all_environments is None:
                st.error("Failed to fetch necessary definitions. Cannot enrich data.")
                st.session_state['runs_df'] = runs_df # Store unenriched data
                st.stop()

            # 3. Create Lookup Mappings
            job_map = {job['id']: {'name': job.get('name', f"Job {job['id']}"),
                                   'project_id': job.get('project_id'),
                                   'environment_id': job.get('environment_id')} 
                       for job in all_jobs}
            project_map = {proj['id']: proj.get('name', f"Project {proj['id']}") for proj in all_projects}
            environment_map = {env['id']: env.get('name', f"Env {env['id']}") for env in all_environments}
            status_map = { 10: "Success", 20: "Error", 30: "Cancelled" }

            # 4. Enrich DataFrame
            st.write("Enriching run data...") 
            if 'status' in runs_df.columns:
                runs_df['Status Name'] = runs_df['status'].map(lambda x: status_map.get(x, f"Unknown ({x})"))
            else:
                 st.warning("Column 'status' not found in run data. Cannot determine status names.")
                 runs_df['Status Name'] = 'Status N/A'
            runs_df['Job Name'] = runs_df['job_definition_id'].map(lambda x: job_map.get(x, {}).get('name', f"Unknown Job {x}"))
            runs_df['Project ID'] = runs_df['job_definition_id'].map(lambda x: job_map.get(x, {}).get('project_id'))
            runs_df['Environment ID'] = runs_df['job_definition_id'].map(lambda x: job_map.get(x, {}).get('environment_id'))
            runs_df['Project Name'] = runs_df['Project ID'].map(lambda x: project_map.get(x, f"Unknown Project {x}"))
            runs_df['Environment Name'] = runs_df['Environment ID'].map(lambda x: environment_map.get(x, f"Unknown Env {x}"))

            # 5. Select and Store Final DataFrame
            cols_to_keep = [
                'id', 'Status Name', 'Job Name', 'Project Name', 'Environment Name',
                'created_at', 'duration', 'job_definition_id', 'status', 
                'git_branch', 'git_sha'
            ]
            existing_cols = [col for col in cols_to_keep if col in runs_df.columns]
            final_df = runs_df[existing_cols].copy()
            # Sort by creation time descending for consistent display
            final_df = final_df.sort_values(by='created_at', ascending=False)

            st.session_state['runs_df'] = final_df # Update session state
            st.success(f"Successfully fetched and enriched {len(final_df)} runs for the selected range.")
            st.rerun()

# --- Display Data and Filters (if data exists in session state) ---
if not st.session_state.runs_df.empty:
    df = st.session_state.runs_df.copy() 

    # Prepare filter options from the DataFrame
    available_statuses = sorted(df['Status Name'].unique())
    available_projects = sorted(df['Project Name'].unique())
    available_environments = sorted(df['Environment Name'].unique())
    available_jobs = sorted(df['Job Name'].unique())

    with col1: # Filters Sidebar
        st.metric("Total Runs Fetched (Range)", len(df)) # Show total fetched for the range
        st.write("**Filter Runs:**")
        selected_statuses = st.multiselect("Status", available_statuses, default=available_statuses)
        selected_projects = st.multiselect("Project", available_projects, default=available_projects)
        selected_environments = st.multiselect("Environment", available_environments, default=available_environments)
        selected_jobs = st.multiselect("Job Name", available_jobs, default=available_jobs)

    # Apply Filters
    filtered_df = df[
        df['Status Name'].isin(selected_statuses) &
        df['Project Name'].isin(selected_projects) &
        df['Environment Name'].isin(selected_environments) &
        df['Job Name'].isin(selected_jobs)
    ].copy() 

    with col2: # Main display area
        st.write(f"**Range Summary ({start_date.isoformat()} to {end_date.isoformat()})**") # Update title
        # Calculate Metrics from the potentially filtered DataFrame
        total_runs_display = len(filtered_df)
        successful_runs = len(filtered_df[filtered_df['Status Name'] == 'Success'])
        failed_runs = len(filtered_df[filtered_df['Status Name'] == 'Error'])
        cancelled_runs = len(filtered_df[filtered_df['Status Name'] == 'Cancelled'])
        
        # Display Metrics in columns
        metric_cols = st.columns(4)
        with metric_cols[0]: st.metric("Total Runs (Filtered)", total_runs_display)
        with metric_cols[1]: st.metric("Successful", successful_runs, delta_color="off") 
        with metric_cols[2]: st.metric("Failed/Errored", failed_runs, delta_color="inverse" if failed_runs > 0 else "off")
        with metric_cols[3]: st.metric("Cancelled", cancelled_runs, delta_color="off")
        
        st.divider() 
        
        st.write(f"**Run Details ({len(filtered_df)} of {len(df)} runs shown after filtering)**")
        # Define columns to display in the main table
        display_cols = ['id', 'Status Name', 'Job Name', 'Project Name', 'Environment Name', 'created_at', 'duration']
        # Ensure columns exist before trying to display them
        display_cols_existing = [col for col in display_cols if col in filtered_df.columns]
        st.dataframe(filtered_df[display_cols_existing], use_container_width=True, height=600)
        
        st.write("**Run Status Summary (Filtered)**")
        if not filtered_df.empty:
            status_counts = filtered_df['Status Name'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            st.dataframe(status_counts, use_container_width=True)
        else:
             st.write("(No runs match current filters)")

elif 'runs_df' in st.session_state and st.session_state.runs_df.empty:
    # Handles the case where the button was clicked but no runs were found OR filters cleared everything
    with col2:
        st.info("No runs to display. Fetch runs for the selected date or adjust filters.")

# No 'else' needed here, if runs_df isn't in session_state, nothing is displayed yet

# --- Removed the old __main__ block ---
# No longer needed as Streamlit runs the script from the top 
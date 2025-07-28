# dbt Cloud Job Monitor Dashboard

A Streamlit-based dashboard for monitoring dbt Cloud job runs across **multiple dbt projects** and environments.

## Overview

This tool provides real-time visibility into dbt Cloud job execution status, performance metrics, and historical trends **across all your dbt projects**. It connects to the dbt Cloud Administrative API to fetch job run data and presents it in an interactive web interface, giving you a unified view of job performance across your entire dbt ecosystem.

## Features

- **Date Range Analysis**: View runs across any date range with flexible filtering
- **Multi-Dimensional Filtering**: Filter by status, project, environment, and job name
- **Real-time Metrics**: Success/failure/cancellation rates with visual indicators
- **Performance Tracking**: Duration analysis and run history
- **Cross-Project Visibility**: Monitor all projects and environments from one dashboard

## Prerequisites

- Python 3.7+
- dbt Cloud account with API access
- dbt Cloud API token
- dbt Cloud account ID

## Installation

1. Clone or download this repository
2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. Create a `.env` file in the project root with your dbt Cloud credentials:
   ```
   DBT_CLOUD_API_TOKEN=your_api_token_here
   DBT_CLOUD_ACCOUNT_ID=your_account_id_here
   DBT_CLOUD_BASE_URL=https://xyz.getdbt.com
   ```

2. Replace the placeholder values:
   - `DBT_CLOUD_API_TOKEN`: Your dbt Cloud API token (get from account settings)
   - `DBT_CLOUD_ACCOUNT_ID`: Your dbt Cloud account ID (numeric only)
   - `DBT_CLOUD_BASE_URL`: Keep as `https://xyz.getdbt.com` unless using a different region

## Usage

1. Run the Streamlit app:
   ```bash
   streamlit run dbt_job_monitor.py --server.port 3000
   ```

2. Open your browser and navigate to `http://localhost:3000`

3. Select a date range to view job runs

4. Use the filters to narrow down results by status, project, environment, or job name

## API Endpoints Used

The dashboard uses the following dbt Cloud API v2 endpoints:
- `/api/v2/accounts/{account_id}/runs/` - Fetch job runs
- `/api/v2/accounts/{account_id}/jobs/` - Fetch job definitions
- `/api/v2/accounts/{account_id}/projects/` - Fetch project information
- `/api/v2/accounts/{account_id}/environments/` - Fetch environment information

## Data Flow

1. User selects a date range
2. App fetches job runs for each day in the range
3. App fetches related job, project, and environment definitions
4. Data is enriched with human-readable names and status labels
5. Results are displayed in an interactive table with filtering options

## Caching

The app uses Streamlit's caching to improve performance:
- Job definitions: Cached for 1 hour
- Project/environment data: Cached for 1 hour
- Run data: Cached for 10 minutes

## Troubleshooting

**API Token Issues**: Ensure your API token is valid and has the necessary permissions to read job run data.

**Account ID Issues**: Make sure your account ID contains only numbers.

**No Data Displayed**: Check that you have job runs in the selected date range and that your API credentials are correct.

**Port Conflicts**: If port 3000 is in use, specify a different port:
   ```bash
   streamlit run dbt_job_monitor.py --server.port 3001
   ```

## Dependencies

- `streamlit>=1.20.0` - Web framework
- `requests>=2.20.0` - HTTP requests
- `pandas>=1.3.0` - Data manipulation
- `python-dotenv>=0.15.0` - Environment variable loading

## License

This project is for internal use. Please ensure compliance with your organization's data access policies when using this tool. 
# Monitoring dbt Cloud Job Runs via Administrative API

This document summarizes how the dbt Cloud Administrative API can be used to fetch and analyze job run information.

## 1. API and Authentication

*   **API Used:** dbt Cloud Administrative API (v2)
    *   **Why:** This API provides endpoints specifically designed for accessing operational data, including the historical list of job runs and definitions for jobs, projects, and environments.
*   **Authentication:** Service Account Token
    *   **Method:** Provide the token in the `Authorization` HTTP header with the `Bearer` prefix (e.g., `Authorization: Bearer YOUR_SERVICE_TOKEN`).
    *   **Required Permissions:** For fetching run history and related definitions, read-only permissions are sufficient. Recommended permission sets:
        *   **`Job Viewer` (Enterprise Plan):** Most specific, grants read access only to jobs and runs.
        *   **`Account Viewer` (Enterprise Plan):** Broader read access across the account, suitable if you might need to read other details later.
        *   **`Read-only` (Team Plan):** Provides necessary read access.
    *   **Note:** Avoid using highly privileged tokens like `Account Admin` for read-only monitoring tasks.

## 2. Fetching Job Run Data

*   **Primary Endpoint:** `GET /api/v2/accounts/{accountId}/runs/`
    *   **Why:** This is the standard endpoint to retrieve a list of historical job runs for the specified account.
*   **Key Parameters Used:**
    *   `limit`: Controls the number of runs returned per request (e.g., `limit=100`). Used for pagination.
    *   `offset`: Specifies how many records to skip. Used with `limit` to fetch subsequent pages.
    *   `order_by=-created_at`: **Crucial.** Sorts the runs by creation time, newest first. This is essential for efficient client-side date filtering.
*   **Filtering by Date (Client-Side Required):**
    *   **Limitation:** The `/api/v2/accounts/{accountId}/runs/` endpoint **does not** support server-side filtering by date (e.g., a `created_at__gte` parameter is not available).
    *   **Strategy:**
        1.  Fetch runs page by page using `limit` and `offset`, ordered by `created_at` descending.
        2.  For each run received, check its `created_at` timestamp.
        3.  Keep only the runs that fall within the desired date range (e.g., "today").
        4.  **Stop fetching** further pages as soon as a run is encountered whose `created_at` timestamp is *before* the start of the desired date range (because runs are ordered newest first). This avoids fetching unnecessary history.

## 3. Enriching Run Data with Names

Raw run data often contains IDs (like `job_definition_id`). To display human-readable names, additional API calls are needed to fetch definitions:

*   **Fetch Job Definitions:** `GET /api/v2/accounts/{accountId}/jobs/`
    *   **Why:** To get the mapping between a run's `job_definition_id` and the actual **Job Name**. This response also typically includes the `project_id` and `environment_id` associated with the job.
*   **Fetch Project Definitions:** `GET /api/v2/accounts/{accountId}/projects/`
    *   **Why:** To get the mapping between a `project_id` (obtained from the job definition) and the **Project Name**.
*   **Fetch Environment Definitions:** `GET /api/v2/accounts/{accountId}/environments/`
    *   **Why:** To get the mapping between an `environment_id` (obtained from the job definition) and the **Environment Name**.
*   **Process:**
    1.  Fetch all jobs, projects, and environments once (can be cached).
    2.  Create lookup dictionaries (e.g., `job_id -> {name, project_id, env_id}`, `project_id -> name`, `env_id -> name`).
    3.  Use these dictionaries to add 'Job Name', 'Project Name', and 'Environment Name' columns to the run data.

## 4. Handling Status Codes

*   The `status` field in the run data provides a numeric code.
*   **Mapping:** These codes can be mapped to human-readable strings:
    *   `10` -> "Success"
    *   `20` -> "Error" / "Failed"
    *   `30` -> "Cancelled"

## 5. Limitations Encountered

*   **No Server-Side Date Filtering for Runs:** Requires fetching pages and filtering client-side, which can be less efficient if looking for runs far back in history.
*   **Potentially Null `trigger` Information:** In testing, the `trigger` object within the `/runs` response was frequently `null`. This object normally contains details about how the run was initiated (schedule, manual, API, PR). Its absence prevented reliable identification and filtering of *scheduled* runs specifically. The reason for this null value wasn't determined but could be specific to account configuration or run types.
*   **Multiple API Calls for Enrichment:** Getting readable names requires fetching full lists of jobs, projects, and environments, adding complexity and potential initial load time compared to just fetching runs. Caching these definition lookups is recommended.

## Conclusion

The dbt Cloud Administrative API v2 provides the necessary tools to monitor job run history. While some limitations exist (notably the lack of server-side date filtering and potentially missing trigger details), you can effectively retrieve run status, timings, and related job/project/environment information by combining the `/runs` endpoint with definition lookups and implementing client-side filtering. 
# OKR Shift and Check-in Analysis

A Streamlit-based application for analyzing OKR (Objectives and Key Results) progress, tracking check-in behaviors, and generating detailed reports.

## Features

-   **Deep OKR Analysis**: Calculates OKR movement (shift) and health scores.
-   **Check-in Tracking**: Monitors check-in frequency and compliance (Last Week, Monthly).
-   **Strict Scope**: Analysis is strictly scoped to the `nvvanphong` group from Base.vn.
-   **Table API Integration**: Fetches "Next Action Scores" from Base Table (ID 81).
-   **Email Reporting**: detailed HTML email reports to managers or users.
-   **Excel Export**: Generates comprehensive Excel reports.

## Setup

1.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Environment Configuration**
    Create a `.env` file in the root directory with the following credentials:
    ```ini
    GOAL_ACCESS_TOKEN=your_goal_token
    ACCOUNT_ACCESS_TOKEN=your_account_token
    TABLE_ACCESS_TOKEN=your_table_token
    EMAIL_USER=your_email@gmail.com
    EMAIL_PASSWORD=your_app_password
    ```

3.  **Run the Application**
    ```bash
    streamlit run app.py
    ```

## Project Structure

-   `app.py`: Main Streamlit application and UI logic.
-   `goal_new.py`: Core OKR calculation logic.
-   `table_client.py`: Client for interacting with Base Table API.
-   `excel_generator.py`: Excel report generation logic.
-   `requirements.txt`: Python dependencies.

## Recent Updates

-   **Security**: All API tokens and credentials moved to `.env` for security.
-   **Scope**: "Total Users" and "Missing" analysis now strictly uses `v1/group/get` (nvvanphong) to prevent data mismatch.
-   **Visuals**: Improved pie chart logic and added "Last Week" check-in debug tools.

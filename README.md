# OKR & Checkin Analysis Tool

This application is a Streamlit-based dashboard for analyzing OKR (Objectives and Key Results) progress and Check-in data from Base.vn.

## Features

*   **OKR Analysis**: Comprehensive view of OKR progress, tracking "OKR Shift" (Dịch chuyển OKR) month-over-month.
*   **Monthly Logic**: 
    *   Calculates OKR shift based on monthly progress.
    *   Special handling for quarter-start months (always 100% baseline or adjusted).
*   **Check-in Quality Assessment**: 
    *   Integrates with Base Table (ID 81) to fetch "Next Action Scores".
    *   Calculates Median Score to classify check-in quality (High/Medium/Low).
*   **Excel Reporting**: 
    *   Generates a professional "Đánh giá OKRs" Excel report.
    *   Includes automatic scoring for OKR Shift, Check-in frequency, and Quality.
    *   Includes a built-in color-coded legend (Row 40+) for easy reference.

## Installation

1.  Clone the repository.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

The application requires Base.vn API tokens. You can set them in a `.env` file in the root directory:

```env
GOAL_ACCESS_TOKEN=your_goal_access_token
ACCOUNT_ACCESS_TOKEN=your_account_access_token
TABLE_ACCESS_TOKEN=your_table_access_token_id_81
```

## Usage

1.  Run the Streamlit app:
    ```bash
    streamlit run app.py
    ```
2.  **Sidebar Interactions**:
    *   Select the **Cycle** (Chu kỳ) for analysis.
    *   Configure **Email** recipients if sending reports.
3.  **Analysis**:
    *   The app will automatically load and process data.
    *   View "Điểm số người dùng" to see the analysis.
4.  **Export**:
    *   Click **"📋 Export to Excel Format"** to download the comprehensive monthly report.
    *   The report includes the new Legend and Median Score data.
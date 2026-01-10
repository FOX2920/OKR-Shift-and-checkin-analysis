# 🎯 OKR & Check-in Analysis Dashboard

A comprehensive Streamlit application for analyzing Objectives and Key Results (OKR) progress and Check-in behaviors within your organization. This tool integrates with **Base.vn** APIs to provide real-time insights, visualizations, and automated reporting.

## ✨ Features

- **📊 Real-time Dashboard**: Visualize OKR progress and check-in statistics in real-time.
- **🔄 OKR Shift Analysis**: Track weekly and monthly movements in OKR scores to identify progress (📈), stability (➡️), or risks (📉).
- **📝 Check-in Monitoring**: Analyze check-in frequency and compliance (e.g., users with goals but no check-ins).
- **📧 Automated Reporting**: Generate and send detailed HTML email reports to managers or the entire company.
- **📈 Visual Charts**: Interactive charts using Plotly for deep dives into data.
- **💾 Excel Export**: Export detailed analysis data for further offline processing.

## 🛠️ Prerequisites

- **Python 3.8+**
- **Base.vn API Tokens**:
    - `GOAL_ACCESS_TOKEN`: For accessing OKR/Goal data.
    - `ACCOUNT_ACCESS_TOKEN`: For accessing user account data.

## 📦 Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/FOX2920/OKR-Shift-and-checkin-analysis.git
    cd OKR-Shift-and-checkin-analysis
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## 🚀 Usage

Run the Streamlit application:

```bash
python -m streamlit run app.py
```

### Configuration
On the sidebar, you will need to input your **Base.vn API Tokens** to fetch data.

1.  Enter **Goal Access Token**.
2.  Enter **Account Access Token**.
3.  Select the **Cycle** (OKR Quarter) you want to analyze.
4.  Click **"Load & Process Data"**.

## 📊 Reports Available

- **Overview**: General stats on users, goals, and check-in rates.
- **Missing Check-ins**: Lists users who haven't checked in despite having goals.
- **OKR Analysis**: Detailed breakdown of OKR score movements (Weekly/Monthly).
- **Check-in Behavior**: Top performers and "at-risk" users based on check-in frequency.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
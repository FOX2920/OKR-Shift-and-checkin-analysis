"""
OKR & Checkin Analysis Dashboard - Main Application
Streamlit application chính sử dụng các module đã tách riêng
"""
import streamlit as st
import warnings

# Import modules
from ui_components import main

# Configuration
warnings.filterwarnings('ignore')

# Streamlit configuration
st.set_page_config(
    page_title="OKR & Checkin Analysis",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Run main application
if __name__ == "__main__":
    main()

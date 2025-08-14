import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timezone, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
import base64
from io import BytesIO
import plotly.io as pio
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

warnings.filterwarnings('ignore')

# Set page config
st.set_page_config(
    page_title="OKR & Checkin Analysis (Google Sheets)",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

class EnhancedGoogleSheetsOKRAnalyzer:
    """Enhanced OKR Analysis System using Google Sheets as data source with caching and retry logic"""

    def __init__(self, apps_script_url: str):
        self.apps_script_url = apps_script_url
        self.session = requests.Session()
        self.session.timeout = 60
        
        # Data storage
        self.data_cache = {}
        self.last_refresh_time = None
        
        # Initialize all dataframes
        self.final_df = None
        self.filtered_members_df = None
        self.cycles_df = None
        self.goals_df = None
        self.krs_df = None
        self.checkins_df = None
        self.analysis_df = None
        self.insights_df = None
        
        # Connection status
        self.connection_status = "unknown"
        self.last_error = None

    def test_connection(self) -> Tuple[bool, str]:
        """Test connection to Google Apps Script"""
        try:
            st.info("🔄 Testing connection to Google Apps Script...")
            
            response = self.session.get(
                f"{self.apps_script_url}?action=ping",
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success':
                    self.connection_status = "connected"
                    st.success("✅ Connection successful!")
                    return True, "Connection successful"
                else:
                    self.connection_status = "error"
                    error_msg = result.get('message', 'Unknown error')
                    st.error(f"❌ Apps Script error: {error_msg}")
                    return False, error_msg
            else:
                self.connection_status = "error"
                error_msg = f"HTTP {response.status_code}: {response.text}"
                st.error(f"❌ HTTP error: {error_msg}")
                return False, error_msg
                
        except requests.exceptions.Timeout:
            self.connection_status = "timeout"
            error_msg = "Connection timeout - Google Apps Script may be slow"
            st.warning(f"⏱️ {error_msg}")
            return False, error_msg
        except Exception as e:
            self.connection_status = "error"
            error_msg = f"Connection error: {str(e)}"
            st.error(f"❌ {error_msg}")
            self.last_error = error_msg
            return False, error_msg

    def trigger_data_refresh(self) -> bool:
        """Trigger Google Apps Script to refresh all OKR data with progress tracking"""
        try:
            # Create progress tracking
            progress_container = st.container()
            with progress_container:
                st.info("🔄 Triggering data refresh in Google Sheets...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.text("⏳ Connecting to Google Apps Script...")
                progress_bar.progress(0.1)
                
                # Make the request with extended timeout for data fetching
                status_text.text("📊 Fetching data from APIs...")
                progress_bar.progress(0.3)
                
                response = self.session.get(
                    f"{self.apps_script_url}?action=fetchAllOKRData",
                    timeout=300  # 5 minutes timeout for data fetching
                )
                
                progress_bar.progress(0.8)
                status_text.text("🔍 Processing response...")
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get('status') == 'success':
                        progress_bar.progress(1.0)
                        status_text.text("✅ Data refresh completed!")
                        
                        # Clear cache
                        self.data_cache.clear()
                        self.last_refresh_time = datetime.now()
                        
                        time.sleep(1)  # Let user see completion
                        progress_container.empty()
                        st.success("✅ Data refresh completed successfully!")
                        st.info("ℹ️ All sheets have been updated with the latest OKR data.")
                        return True
                    else:
                        progress_container.empty()
                        error_msg = result.get('message', 'Unknown error')
                        st.error(f"❌ Data refresh failed: {error_msg}")
                        return False
                else:
                    progress_container.empty()
                    st.error(f"❌ Failed to trigger data refresh: HTTP {response.status_code}")
                    st.code(response.text)
                    return False
                    
        except requests.exceptions.Timeout:
            st.warning("⏱️ Data refresh is taking longer than expected (>5 minutes).")
            st.info("💡 This is normal for large datasets. Please check the Google Sheets manually after a few minutes.")
            return False
        except Exception as e:
            st.error(f"❌ Error triggering data refresh: {e}")
            return False

    def get_sheet_data_with_retry(self, sheet_name: str, max_retries: int = 3) -> pd.DataFrame:
        """Get data from a specific sheet with retry logic and caching"""
        
        # Check cache first
        cache_key = f"sheet_{sheet_name}"
        if cache_key in self.data_cache:
            cache_time, cached_df = self.data_cache[cache_key]
            # Use cached data if less than 5 minutes old
            if (datetime.now() - cache_time).seconds < 300:
                return cached_df
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    st.info(f"🔄 Retry {attempt + 1}/{max_retries} for {sheet_name}...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                
                response = self.session.get(
                    f"{self.apps_script_url}?action=getSheetData&sheet={sheet_name}",
                    timeout=60
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get('status') == 'success':
                        data = result.get('data', [])
                        if data and len(data) > 1:  # Has headers and data
                            headers = data[0]
                            rows = data[1:]
                            df = pd.DataFrame(rows, columns=headers)
                            
                            # Clean empty rows
                            df = df.dropna(how='all')
                            
                            # Convert numeric columns intelligently
                            df = self._convert_numeric_columns(df)
                            
                            # Cache the result
                            self.data_cache[cache_key] = (datetime.now(), df)
                            
                            return df
                        else:
                            # Empty sheet is valid, cache empty DataFrame
                            empty_df = pd.DataFrame()
                            self.data_cache[cache_key] = (datetime.now(), empty_df)
                            return empty_df
                    else:
                        error_msg = result.get('message', 'Unknown error')
                        if attempt == max_retries - 1:
                            st.error(f"❌ Failed to get {sheet_name} data: {error_msg}")
                        continue
                else:
                    if attempt == max_retries - 1:
                        st.error(f"❌ HTTP error getting {sheet_name}: {response.status_code}")
                    continue
                    
            except requests.exceptions.Timeout:
                if attempt == max_retries - 1:
                    st.warning(f"⏱️ Timeout getting {sheet_name} data after {max_retries} attempts")
                continue
            except Exception as e:
                if attempt == max_retries - 1:
                    st.error(f"❌ Error getting {sheet_name} data: {e}")
                continue
        
        # Return empty DataFrame if all attempts failed
        return pd.DataFrame()

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Intelligently convert numeric columns"""
        if df.empty:
            return df
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Identify potential numeric columns
            if any(keyword in col_lower for keyword in ['value', 'count', 'score', 'rating', 'shift', 'id']):
                # Try to convert to numeric, keeping non-numeric as-is
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # For ID columns, fill NaN with empty string and convert back to string
                if 'id' in col_lower:
                    df[col] = df[col].fillna('').astype(str)
                else:
                    # For value columns, fill NaN with 0
                    df[col] = df[col].fillna(0)
            
            # Convert date columns
            elif any(keyword in col_lower for keyword in ['since', 'date', 'time']):
                # Keep as string for now, convert when needed
                df[col] = df[col].astype(str)
        
        return df

    def load_all_sheets_data_parallel(self, progress_callback=None):
        """Load data from all sheets in parallel for better performance"""
        try:
            sheets_to_load = [
                ('Cycles', 'cycles_df'),
                ('Members', 'filtered_members_df'), 
                ('Goals', 'goals_df'),
                ('KRs', 'krs_df'),
                ('Checkins', 'checkins_df'),
                ('Final_Dataset', 'final_df'),
                ('Analysis', 'analysis_df'),
                ('Insights', 'insights_df')
            ]
            
            total_sheets = len(sheets_to_load)
            loaded_count = 0
            
            # Load sheets sequentially to avoid overwhelming the server
            for sheet_name, attr_name in sheets_to_load:
                if progress_callback:
                    progress_callback(f"Loading {sheet_name} data...", loaded_count / total_sheets)
                
                df = self.get_sheet_data_with_retry(sheet_name)
                setattr(self, attr_name, df)
                
                loaded_count += 1
                
                if not df.empty:
                    st.success(f"✅ Loaded {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
                else:
                    st.warning(f"⚠️ {sheet_name} is empty or failed to load")
                
                # Small delay to avoid overwhelming the server
                time.sleep(0.5)
            
            if progress_callback:
                progress_callback("Finalizing data load...", 1.0)
            
            # Validate critical sheets
            self._validate_loaded_data()
            
            return self.final_df
            
        except Exception as e:
            st.error(f"❌ Error loading sheets data: {e}")
            return None

    def _validate_loaded_data(self):
        """Validate that critical data has been loaded correctly"""
        validation_results = []
        
        # Check Final_Dataset
        if self.final_df is not None and not self.final_df.empty:
            validation_results.append(f"✅ Final Dataset: {len(self.final_df)} rows")
        else:
            validation_results.append("❌ Final Dataset: Empty or missing")
        
        # Check Analysis
        if self.analysis_df is not None and not self.analysis_df.empty:
            validation_results.append(f"✅ Analysis: {len(self.analysis_df)} rows")
        else:
            validation_results.append("⚠️ Analysis: Empty or missing")
        
        # Check Insights
        if self.insights_df is not None and not self.insights_df.empty:
            validation_results.append(f"✅ Insights: {len(self.insights_df)} rows")
        else:
            validation_results.append("⚠️ Insights: Empty or missing")
        
        # Check Members
        if self.filtered_members_df is not None and not self.filtered_members_df.empty:
            validation_results.append(f"✅ Members: {len(self.filtered_members_df)} rows")
        else:
            validation_results.append("❌ Members: Empty or missing")
        
        # Display validation results
        st.subheader("📊 Data Validation Results")
        for result in validation_results:
            if "✅" in result:
                st.success(result)
            elif "⚠️" in result:
                st.warning(result)
            else:
                st.error(result)

    def get_available_cycles(self) -> List[Dict]:
        """Get available cycles with better error handling"""
        try:
            if self.cycles_df is None or self.cycles_df.empty:
                st.info("🔄 Loading cycles data...")
                cycles_df = self.get_sheet_data_with_retry('Cycles')
                self.cycles_df = cycles_df
            
            if self.cycles_df.empty:
                st.warning("⚠️ No cycles data available")
                return []
            
            cycles = []
            for _, row in self.cycles_df.iterrows():
                try:
                    # Parse the formatted_start_time
                    start_time_str = row.get('formatted_start_time', '')
                    if start_time_str:
                        start_time = datetime.strptime(start_time_str, '%d/%m/%Y')
                    else:
                        start_time = datetime.now()
                    
                    cycles.append({
                        'name': row.get('name', ''),
                        'path': row.get('path', ''),
                        'start_time': start_time,
                        'formatted_start_time': start_time_str
                    })
                except Exception as e:
                    st.warning(f"⚠️ Error parsing cycle {row.get('name', 'Unknown')}: {e}")
                    continue
            
            # Sort by start_time descending (latest first)
            cycles.sort(key=lambda x: x['start_time'], reverse=True)
            
            if cycles:
                st.success(f"✅ Found {len(cycles)} cycles")
            
            return cycles
            
        except Exception as e:
            st.error(f"❌ Error getting cycles: {e}")
            return []

    def get_data_freshness_info(self) -> Dict:
        """Get information about data freshness"""
        info = {
            'last_refresh': self.last_refresh_time,
            'cache_entries': len(self.data_cache),
            'connection_status': self.connection_status,
            'last_error': self.last_error
        }
        
        # Get cache ages
        cache_ages = {}
        for key, (cache_time, _) in self.data_cache.items():
            age_minutes = (datetime.now() - cache_time).seconds // 60
            cache_ages[key] = age_minutes
        
        info['cache_ages'] = cache_ages
        return info

    def clear_cache(self):
        """Clear all cached data"""
        self.data_cache.clear()
        st.success("✅ Cache cleared successfully")

    def get_last_friday_date(self) -> datetime:
        """Get last Friday date"""
        today = datetime.now()
        day_of_week = today.weekday()  # 0 = Monday, 4 = Friday, 6 = Sunday
        
        # Calculate days to subtract to get to last Friday
        if day_of_week == 4:  # Today is Friday
            days_to_subtract = 7  # Last Friday (a week ago)
        elif day_of_week == 5:  # Today is Saturday
            days_to_subtract = 1  # Yesterday (Friday)
        elif day_of_week == 6:  # Today is Sunday
            days_to_subtract = 2  # Friday before yesterday
        else:  # Monday to Thursday
            days_to_subtract = day_of_week + 3  # Days since last Friday
        
        last_friday = today - timedelta(days=days_to_subtract)
        last_friday = last_friday.replace(hour=23, minute=59, second=59, microsecond=999000)
        
        return last_friday

    def get_quarter_start_date(self) -> datetime:
        """Get current quarter start date"""
        today = datetime.now()
        quarter = (today.month - 1) // 3 + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        return datetime(today.year, quarter_start_month, 1)

    def analyze_missing_goals_and_checkins(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Analyze members without goals and without checkins with better error handling"""
        try:
            if self.filtered_members_df is None or self.filtered_members_df.empty:
                st.warning("⚠️ Filtered members data not available")
                return [], [], []
            
            if self.final_df is None or self.final_df.empty:
                st.warning("⚠️ Final dataset not available")
                return [], [], []

            # Get users with goals
            users_with_goals = set(self.final_df['goal_user_name'].dropna().unique())
            
            # Get users with checkins
            users_with_checkins = set()
            if 'checkin_user_id' in self.final_df.columns:
                # Map user IDs to names for checkins
                user_id_to_name = dict(zip(self.filtered_members_df['id'], self.filtered_members_df['name']))
                checkin_user_ids = self.final_df['checkin_user_id'].dropna().unique()
                users_with_checkins = {user_id_to_name.get(str(uid), str(uid)) for uid in checkin_user_ids if str(uid) in user_id_to_name}
            elif 'checkin_name' in self.final_df.columns:
                # Alternative: use goal_user_name for users who have made checkins
                users_with_checkins = set(self.final_df[
                    (self.final_df['checkin_name'].notna()) & 
                    (self.final_df['checkin_name'] != '')
                ]['goal_user_name'].dropna().unique())
            
            # Get all filtered members
            all_members = set(self.filtered_members_df['name'].unique())
            
            # Find missing groups
            members_without_goals = []
            members_without_checkins = []
            members_with_goals_no_checkins = []
            
            for member_name in all_members:
                member_info = self.filtered_members_df[self.filtered_members_df['name'] == member_name]
                if member_info.empty:
                    continue
                    
                member_dict = member_info.iloc[0].to_dict()
                
                has_goal = member_name in users_with_goals
                has_checkin = member_name in users_with_checkins
                
                if not has_goal:
                    members_without_goals.append({
                        'name': member_name,
                        'username': member_dict.get('username', ''),
                        'job': member_dict.get('job', ''),
                        'email': member_dict.get('email', ''),
                        'id': member_dict.get('id', '')
                    })
                
                if not has_checkin:
                    members_without_checkins.append({
                        'name': member_name,
                        'username': member_dict.get('username', ''),
                        'job': member_dict.get('job', ''),
                        'email': member_dict.get('email', ''),
                        'id': member_dict.get('id', ''),
                        'has_goal': has_goal
                    })
                
                if has_goal and not has_checkin:
                    members_with_goals_no_checkins.append({
                        'name': member_name,
                        'username': member_dict.get('username', ''),
                        'job': member_dict.get('job', ''),
                        'email': member_dict.get('email', ''),
                        'id': member_dict.get('id', '')
                    })
            
            # Show summary
            st.success(f"✅ Analysis complete: {len(members_without_goals)} without goals, {len(members_without_checkins)} without checkins")
            
            return members_without_goals, members_without_checkins, members_with_goals_no_checkins
            
        except Exception as e:
            st.error(f"❌ Error analyzing missing goals and checkins: {e}")
            return [], [], []

    def get_okr_shifts_from_insights(self) -> List[Dict]:
        """Get OKR shifts data from Insights sheet with validation"""
        try:
            if self.insights_df is None or self.insights_df.empty:
                st.info("🔄 Loading insights data...")
                insights_df = self.get_sheet_data_with_retry('Insights')
                self.insights_df = insights_df
            
            if self.insights_df.empty:
                st.warning("⚠️ Insights data not available")
                return []
            
            okr_shifts = []
            for _, row in self.insights_df.iterrows():
                try:
                    current_value = float(row.get('final_goal_value', 0))
                    last_friday_value = float(row.get('last_friday_final_goal_value', 0))
                    okr_shift = current_value - last_friday_value
                    
                    okr_shifts.append({
                        'user_name': row.get('goal_user_name', ''),
                        'current_value': round(current_value, 2),
                        'last_friday_value': round(last_friday_value, 2),
                        'okr_shift': round(okr_shift, 2),
                        'checkin_count': int(row.get('checkin_count', 0)),
                        'baseline_strategy': 'google_sheets_calculated'
                    })
                except Exception as e:
                    st.warning(f"⚠️ Error processing insights row: {e}")
                    continue
            
            # Sort by OKR shift descending
            okr_shifts.sort(key=lambda x: x['okr_shift'], reverse=True)
            
            if okr_shifts:
                st.success(f"✅ Loaded {len(okr_shifts)} OKR shift records")
            
            return okr_shifts
            
        except Exception as e:
            st.error(f"❌ Error getting OKR shifts from insights: {e}")
            return []

    def analyze_checkin_behavior_from_analysis(self) -> Tuple[List[Dict], List[Dict]]:
        """Analyze checkin behavior using Analysis sheet data with improved error handling"""
        try:
            if self.analysis_df is None or self.analysis_df.empty:
                st.info("🔄 Loading analysis data...")
                analysis_df = self.get_sheet_data_with_retry('Analysis')
                self.analysis_df = analysis_df
            
            if self.analysis_df.empty:
                st.warning("⚠️ Analysis data not available")
                return [], []
            
            last_friday = self.get_last_friday_date()
            quarter_start = self.get_quarter_start_date()
            
            # Convert checkin_since to datetime
            df = self.analysis_df.copy()
            df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')
            
            # Filter period data
            mask_period = (df['checkin_since_dt'] >= quarter_start) & (df['checkin_since_dt'] <= last_friday)
            period_df = df[mask_period].copy()
            
            # Get all users
            all_users = df['goal_user_name'].dropna().unique()
            
            # Period analysis
            period_checkins = []
            for user in all_users:
                user_period_data = period_df[period_df['goal_user_name'] == user]
                user_all_data = df[df['goal_user_name'] == user]
                
                # Count checkins in period (non-empty checkin_name)
                checkin_count_period = len(user_period_data[
                    (user_period_data['checkin_name'].notna()) & 
                    (user_period_data['checkin_name'] != '')
                ])
                
                # Count total KRs for this user
                kr_count_period = user_period_data['kr_id'].nunique() if not user_period_data.empty else 0
                kr_count_period = max(kr_count_period, user_all_data['kr_id'].nunique()) if not user_all_data.empty else 0
                
                checkin_rate = (checkin_count_period / kr_count_period * 100) if kr_count_period > 0 else 0
                
                # Get checkin dates
                user_checkin_dates = user_period_data[
                    (user_period_data['checkin_name'].notna()) & 
                    (user_period_data['checkin_name'] != '')
                ]['checkin_since_dt'].dropna()
                
                first_checkin_period = user_checkin_dates.min() if len(user_checkin_dates) > 0 else None
                last_checkin_period = user_checkin_dates.max() if len(user_checkin_dates) > 0 else None
                
                days_between = (last_checkin_period - first_checkin_period).days if first_checkin_period and last_checkin_period else 0
                
                period_checkins.append({
                    'user_name': user,
                    'checkin_count_period': checkin_count_period,
                    'kr_count_period': kr_count_period,
                    'checkin_rate_period': checkin_rate,
                    'first_checkin_period': first_checkin_period,
                    'last_checkin_period': last_checkin_period,
                    'days_between_checkins': days_between
                })
            
            # Overall analysis
            overall_checkins = []
            for user in all_users:
                user_data = df[df['goal_user_name'] == user]
                
                # Count total checkins
                total_checkins = len(user_data[
                    (user_data['checkin_name'].notna()) & 
                    (user_data['checkin_name'] != '')
                ])
                
                total_krs = user_data['kr_id'].nunique() if not user_data.empty else 0
                checkin_rate = (total_checkins / total_krs * 100) if total_krs > 0 else 0
                
                # Get all checkin dates
                user_checkin_dates = user_data[
                    (user_data['checkin_name'].notna()) & 
                    (user_data['checkin_name'] != '')
                ]['checkin_since_dt'].dropna()
                
                first_checkin = user_checkin_dates.min() if len(user_checkin_dates) > 0 else None
                last_checkin = user_checkin_dates.max() if len(user_checkin_dates) > 0 else None
                days_active = (last_checkin - first_checkin).days if first_checkin and last_checkin else 0
                
                checkin_frequency = (total_checkins / (days_active / 7)) if days_active > 0 else 0
                
                overall_checkins.append({
                    'user_name': user,
                    'total_checkins': total_checkins,
                    'total_krs': total_krs,
                    'checkin_rate': checkin_rate,
                    'first_checkin': first_checkin,
                    'last_checkin': last_checkin,
                    'days_active': days_active,
                    'checkin_frequency_per_week': checkin_frequency
                })
            
            # Sort results
            period_checkins.sort(key=lambda x: x['checkin_count_period'], reverse=True)
            overall_checkins.sort(key=lambda x: x['total_checkins'], reverse=True)
            
            st.success(f"✅ Checkin analysis complete: {len(period_checkins)} users analyzed")
            
            return period_checkins, overall_checkins
            
        except Exception as e:
            st.error(f"❌ Error analyzing checkin behavior: {e}")
            return [], []


# Existing EmailReportGenerator class remains the same
class EmailReportGenerator:
    """Generate and send email reports for OKR analysis"""
    
    def __init__(self, smtp_server="smtp.gmail.com", smtp_port=587):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def create_chart_image(self, fig, filename="chart"):
        """Convert plotly figure to bytes for email attachment"""
        try:
            img_bytes = pio.to_image(fig, format="png", width=800, height=600, scale=2)
            return img_bytes
        except Exception as e:
            st.error(f"Error creating chart image: {e}")
            return None

    def create_visual_html_chart(self, data, chart_type, title):
        """Create HTML-based visual charts as fallback"""
        if chart_type == "pie":
            total = sum(data.values())
            if total == 0:
                return f"<div class='chart-fallback'><h4>{title}</h4><p>Không có dữ liệu</p></div>"
            
            html = f"""
            <div class='modern-chart'>
                <h3 style='text-align: center; margin-bottom: 30px; color: #2c3e50; font-size: 20px;'>{title}</h3>
                <div style='display: flex; justify-content: center; align-items: center; gap: 40px; padding: 20px;'>
            """
            
            colors = ['#27AE60', '#E74C3C', '#3498DB', '#F39C12', '#9B59B6']
            
            for i, (label, value) in enumerate(data.items()):
                percentage = (value / total * 100) if total > 0 else 0
                color = colors[i % len(colors)]
                
                # Scale circle size based on value
                circle_size = max(100, min(140, 100 + (value / total * 40)))
                font_size = max(20, min(28, 20 + (value / total * 8)))
                
                html += f"""
                <div style='text-align: center; flex: 1; max-width: 200px;'>
                    <div style='width: {circle_size}px; height: {circle_size}px; border-radius: 50%; 
                                background: linear-gradient(135deg, {color}, {color}dd); 
                                margin: 0 auto 15px auto; display: flex; align-items: center; 
                                justify-content: center; color: white; font-weight: bold; 
                                font-size: {font_size}px; box-shadow: 0 8px 25px rgba(0,0,0,0.15);
                                border: 4px solid white; position: relative; overflow: hidden;'>
                        <span style='z-index: 2; position: relative;'>{value}</span>
                        <div style='position: absolute; top: 0; left: 0; right: 0; bottom: 0; 
                                    background: rgba(255,255,255,0.1); border-radius: 50%;'></div>
                    </div>
                    <div style='font-weight: bold; margin-bottom: 8px; color: #2c3e50; font-size: 16px;'>{label}</div>
                    <div style='color: #7f8c8d; font-size: 14px; background: #ecf0f1; padding: 4px 12px; 
                                border-radius: 15px; display: inline-block;'>{percentage:.1f}%</div>
                </div>
                """
            
            html += "</div></div>"
            return html
            
        elif chart_type == "bar":
            if not data:
                return f"<div class='modern-chart'><h3>{title}</h3><p>Không có dữ liệu</p></div>"
            
            max_value = max(abs(v) for v in data.values()) if data.values() else 1
            
            html = f"""
            <div class='modern-chart'>
                <h3 style='text-align: center; margin-bottom: 25px; color: #2c3e50; font-size: 20px;'>{title}</h3>
                <div style='max-height: 500px; overflow-y: auto; padding: 10px;'>
            """
            
            for i, (name, value) in enumerate(list(data.items())[:15]):  # Top 15
                width_pct = (abs(value) / max_value * 100) if max_value > 0 else 0
                
                if value > 0:
                    color = '#27AE60'
                    bg_color = 'rgba(39, 174, 96, 0.1)'
                    icon = '📈'
                elif value < 0:
                    color = '#E74C3C'
                    bg_color = 'rgba(231, 76, 60, 0.1)'
                    icon = '📉'
                else:
                    color = '#F39C12'
                    bg_color = 'rgba(243, 156, 18, 0.1)'
                    icon = '➡️'
                
                html += f"""
                <div style='margin-bottom: 20px; padding: 15px; background: {bg_color}; 
                            border-radius: 12px; border-left: 4px solid {color};
                            box-shadow: 0 2px 8px rgba(0,0,0,0.05);'>
                    <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                        <strong style='color: #2c3e50; font-size: 15px;'>{name}</strong>
                        <span style='color: {color}; font-weight: bold; font-size: 16px;'>
                            {icon} {value:.2f}
                        </span>
                    </div>
                    <div style='background: rgba(255,255,255,0.8); height: 12px; border-radius: 6px; overflow: hidden;'>
                        <div style='background: {color}; height: 100%; width: {width_pct}%; 
                                    border-radius: 6px; transition: width 0.3s ease;
                                    box-shadow: inset 0 1px 2px rgba(0,0,0,0.1);'></div>
                    </div>
                </div>
                """
            
            html += "</div></div>"
            return html
        
        return f"<div class='modern-chart'><h3>{title}</h3><p>Loại biểu đồ không được hỗ trợ</p></div>"

    def create_email_content(self, analyzer, selected_cycle, members_without_goals, members_without_checkins, 
                           members_with_goals_no_checkins, okr_shifts):
        """Create HTML email content with fallback charts"""
        
        current_date = datetime.now().strftime("%d/%m/%Y")
        total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
        
        # Calculate statistics
        members_with_goals = total_members - len(members_without_goals)
        members_with_checkins = total_members - len(members_without_checkins)
        
        progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0]) if okr_shifts else 0
        stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0]) if okr_shifts else 0
        issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0]) if okr_shifts else 0
        
        # Create visual charts
        goal_chart = self.create_visual_html_chart(
            {'Có OKR': members_with_goals, 'Chưa có OKR': len(members_without_goals)},
            'pie', 'Phân bố trạng thái OKR'
        )
        
        # Create checkin table instead of chart
        checkins_table = self._generate_table_html(members_without_checkins,
                                                 ["Tên", "Username", "Chức vụ", "Có OKR"],
                                                 ["name", "username", "job", "has_goal"])
        
        okr_shifts_data = {u['user_name']: u['okr_shift'] for u in okr_shifts[:15]} if okr_shifts else {}
        okr_shifts_chart = self.create_visual_html_chart(
            okr_shifts_data, 'bar', 'Dịch chuyển OKR của nhân viên (Top 15)'
        )
        
        # Generate tables
        goals_table = self._generate_table_html(members_without_goals, 
                                               ["Tên", "Username", "Chức vụ"], 
                                               ["name", "username", "job"])
        
        goals_no_checkins_table = self._generate_table_html(members_with_goals_no_checkins,
                                                          ["Tên", "Username", "Chức vụ"],
                                                          ["name", "username", "job"])
        
        # Top performers table
        top_performers = [u for u in okr_shifts if u['okr_shift'] > 0][:10] if okr_shifts else []
        top_performers_table = self._generate_okr_table_html(top_performers)
        
        # Issue users table
        issue_performers = [u for u in okr_shifts if u['okr_shift'] < 0][:10] if okr_shifts else []
        issue_performers_table = self._generate_okr_table_html(issue_performers)
        
        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #2c3e50; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f8f9fa; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; border-radius: 15px; text-align: center; margin-bottom: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.15); }}
                .header h1 {{ margin: 0 0 10px 0; font-size: 28px; font-weight: 700; }}
                .header h2 {{ margin: 0 0 10px 0; font-size: 22px; font-weight: 500; opacity: 0.9; }}
                .header p {{ margin: 0; font-size: 16px; opacity: 0.8; }}
                .section {{ background: white; padding: 30px; margin: 25px 0; border-radius: 15px; box-shadow: 0 5px 20px rgba(0,0,0,0.08); border: 1px solid #e9ecef; }}
                .section h2 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; margin-bottom: 25px; font-size: 22px; }}
                .metrics {{ display: flex; justify-content: space-around; margin: 25px 0; flex-wrap: wrap; gap: 15px; }}
                .metric {{ background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%); padding: 25px; border-radius: 12px; text-align: center; box-shadow: 0 4px 15px rgba(0,0,0,0.08); min-width: 140px; flex: 1; border: 1px solid #e9ecef; }}
                .metric-value {{ font-size: 32px; font-weight: 700; color: #3498db; margin-bottom: 5px; }}
                .metric-label {{ font-size: 14px; color: #7f8c8d; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.05); }}
                th {{ padding: 16px; text-align: left; background: linear-gradient(135deg, #3498db, #2980b9); color: white; font-weight: 600; font-size: 14px; text-transform: uppercase; letter-spacing: 0.5px; }}
                td {{ padding: 14px 16px; border-bottom: 1px solid #ecf0f1; font-size: 14px; }}
                tr:nth-child(even) {{ background: #f8f9fa; }}
                tr:hover {{ background: #e8f4f8; transition: background 0.2s ease; }}
                .chart-container {{ text-align: center; margin: 30px 0; }}
                .modern-chart {{ background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%); padding: 30px; border-radius: 15px; box-shadow: 0 8px 25px rgba(0,0,0,0.1); margin: 25px 0; border: 1px solid #e9ecef; }}
                .chart-fallback {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); margin: 20px 0; }}
                .positive {{ color: #27AE60; font-weight: bold; }}
                .negative {{ color: #E74C3C; font-weight: bold; }}
                .neutral {{ color: #F39C12; font-weight: bold; }}
                .footer {{ text-align: center; margin-top: 40px; padding: 25px; background: linear-gradient(135deg, #2c3e50, #34495e); color: white; border-radius: 15px; }}
                .alert {{ padding: 18px; margin: 20px 0; border-radius: 10px; border-left: 4px solid; }}
                .alert-warning {{ background: linear-gradient(135deg, #fff3cd, #fef8e6); border-left-color: #f39c12; color: #856404; }}
                .alert-info {{ background: linear-gradient(135deg, #d1ecf1, #e8f5f7); border-left-color: #3498db; color: #0c5460; }}
                .alert strong {{ font-weight: 600; }}
                @media (max-width: 768px) {{
                    .metrics {{ flex-direction: column; }}
                    .modern-chart {{ padding: 20px; }}
                    .section {{ padding: 20px; }}
                    table {{ font-size: 12px; }}
                    th, td {{ padding: 10px 8px; }}
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 BÁO CÁO TIẾN ĐỘ OKR & CHECKIN</h1>
                <h2>{selected_cycle['name']}</h2>
                <p>Ngày báo cáo: {current_date} | Nguồn: Google Sheets</p>
            </div>
            
            <div class="section">
                <h2>📈 TỔNG QUAN</h2>
                <div class="metrics">
                    <div class="metric">
                        <div class="metric-value">{total_members}</div>
                        <div class="metric-label">Tổng nhân viên</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{members_with_goals}</div>
                        <div class="metric-label">Có OKR</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{members_with_checkins}</div>
                        <div class="metric-label">Có Checkin</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{progress_users}</div>
                        <div class="metric-label">Tiến bộ</div>
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>📝 DANH SÁCH NHÂN VIÊN CHƯA CHECKIN</h2>
                <div class="chart-container">
                    {checkins_table}
                </div>
                <div class="alert alert-info">
                    <strong>Thống kê:</strong> {members_with_checkins}/{total_members} nhân viên đã có Checkin ({(members_with_checkins/total_members*100):.1f}%)
                </div>
            </div>
            
            <div class="section">
                <h2>📊 DỊCH CHUYỂN OKR</h2>
                <div class="chart-container">
                    {okr_shifts_chart}
                </div>
                <div class="metrics">
                    <div class="metric">
                        <div class="metric-value positive">{progress_users}</div>
                        <div class="metric-label">Tiến bộ</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value neutral">{stable_users}</div>
                        <div class="metric-label">Ổn định</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value negative">{issue_users}</div>
                        <div class="metric-label">Cần quan tâm</div>
                    </div>
                </div>
            </div>
        """
        
        # Add detailed tables
        if members_without_goals:
            html_content += f"""
            <div class="section">
                <h2>🚫 NHÂN VIÊN CHƯA CÓ OKR ({len(members_without_goals)} người)</h2>
                <div class="alert alert-warning">
                    <strong>Cần hành động:</strong> Những nhân viên này cần được hỗ trợ thiết lập OKR.
                </div>
                {goals_table}
            </div>
            """
        
        if members_with_goals_no_checkins:
            html_content += f"""
            <div class="section">
                <h2>⚠️ CÓ OKR NHƯNG CHƯA CHECKIN ({len(members_with_goals_no_checkins)} người)</h2>
                <div class="alert alert-warning">
                    <strong>Ưu tiên cao:</strong> Đã có mục tiêu nhưng chưa cập nhật tiến độ.
                </div>
                {goals_no_checkins_table}
            </div>
            """
        
        if top_performers:
            html_content += f"""
            <div class="section">
                <h2>🏆 TOP NHÂN VIÊN TIẾN BỘ NHẤT</h2>
                {top_performers_table}
            </div>
            """
        
        if issue_performers:
            html_content += f"""
            <div class="section">
                <h2>⚠️ NHÂN VIÊN CẦN HỖ TRỢ</h2>
                <div class="alert alert-warning">
                    <strong>Cần quan tâm:</strong> OKR của những nhân viên này đang giảm hoặc không tiến triển.
                </div>
                {issue_performers_table}
            </div>
            """
        
        html_content += """
            <div class="footer">
                <p><strong>🏢 A Plus Mineral Material Corporation</strong></p>
                <p>📊 Báo cáo được tạo tự động bởi hệ thống OKR Analysis (Google Sheets)</p>
                <p><em>📧 Đây là email tự động, vui lòng không trả lời email này.</em></p>
            </div>
        </body>
        </html>
        """
        
        return html_content

    def _generate_table_html(self, data, headers, fields):
        """Generate HTML table from data"""
        if not data:
            return "<div style='text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; color: #7f8c8d;'><p>📭 Không có dữ liệu</p></div>"
        
        html = "<table><thead><tr>"
        for header in headers:
            html += f"<th>{header}</th>"
        html += "</tr></thead><tbody>"
        
        for i, item in enumerate(data):
            row_class = "even" if i % 2 == 0 else "odd"
            html += f"<tr class='{row_class}'>"
            for field in fields:
                value = item.get(field, "")
                if field == "has_goal":
                    value = "<span style='color: #27AE60; font-weight: bold;'>✅ Có</span>" if value else "<span style='color: #E74C3C; font-weight: bold;'>❌ Không</span>"
                html += f"<td>{value}</td>"
            html += "</tr>"
        
        html += "</tbody></table>"
        return html

    def _generate_okr_table_html(self, data):
        """Generate HTML table for OKR data"""
        if not data:
            return "<div style='text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; color: #7f8c8d;'><p>📭 Không có dữ liệu</p></div>"
        
        html = """
        <table>
            <thead>
                <tr>
                    <th>👤 Nhân viên</th>
                    <th>📊 Dịch chuyển</th>
                    <th>🎯 Giá trị hiện tại</th>
                    <th>📅 Giá trị trước đó</th>
                    <th>📝 Số checkin</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for i, item in enumerate(data):
            shift_class = "positive" if item['okr_shift'] > 0 else "negative" if item['okr_shift'] < 0 else "neutral"
            shift_icon = "📈" if item['okr_shift'] > 0 else "📉" if item['okr_shift'] < 0 else "➡️"
            row_class = "even" if i % 2 == 0 else "odd"
            
            html += f"""
            <tr class='{row_class}'>
                <td><strong>{item['user_name']}</strong></td>
                <td class="{shift_class}">{shift_icon} <strong>{item['okr_shift']:.2f}</strong></td>
                <td><span style='color: #3498db; font-weight: 600;'>{item['current_value']:.2f}</span></td>
                <td><span style='color: #7f8c8d;'>{item['last_friday_value']:.2f}</span></td>
                <td><span style='color: #9b59b6; font-weight: 600;'>{item.get('checkin_count', 0)}</span></td>
            </tr>
            """
        
        html += "</tbody></table>"
        return html

    def send_email_report(self, email_from, password, email_to, subject, html_content, 
                         company_name="A Plus Mineral Material Corporation"):
        """Send email report with improved compatibility"""
        try:
            # Create message
            message = MIMEMultipart('related')  # Changed to 'related' for better image support
            message['From'] = f"OKR System {company_name} <{email_from}>"
            message['To'] = email_to
            message['Subject'] = subject
            
            # Create message container
            msg_alternative = MIMEMultipart('alternative')
            message.attach(msg_alternative)
            
            # Add HTML content
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg_alternative.attach(html_part)
            
            # Connect to SMTP server
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(email_from, password)
            
            # Send email
            server.send_message(message)
            server.quit()
            
            return True, "Email sent successfully!"
            
        except smtplib.SMTPAuthenticationError:
            return False, "Lỗi xác thực: Vui lòng kiểm tra lại email và mật khẩu"
        except Exception as e:
            return False, f"Lỗi gửi email: {str(e)}"


# ==================== ENHANCED STREAMLIT APP ====================

def main():
    st.title("🎯 Enhanced OKR & Checkin Analysis Dashboard")
    st.markdown("**🔗 Direct integration with Google Sheets via Apps Script**")
    st.markdown("---")
    
    # Google Apps Script URL
    apps_script_url = "https://script.google.com/macros/s/AKfycbwmRWAOSIxG3CI_rc98gcf4SHf3cA436p3japFPhzuykzYZvSUNyGsFnc6Vjl_cq1yftA/exec"
    
    # Initialize analyzer
    try:
        analyzer = EnhancedGoogleSheetsOKRAnalyzer(apps_script_url)
        email_generator = EmailReportGenerator()
    except Exception as e:
        st.error(f"❌ Failed to initialize analyzer: {e}")
        return

    # Sidebar for configuration and status
    with st.sidebar:
        st.header("⚙️ System Configuration")
        
        # Connection status
        st.subheader("🔗 Connection Status")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🏆 Most Active (Period)")
            if not period_df.empty:
                top_period = period_df.nlargest(10, 'checkin_count_period')[['user_name', 'checkin_count_period', 'checkin_rate_period']].round(1)
                
                # Enhanced bar chart
                fig = px.bar(
                    top_period,
                    x='checkin_count_period',
                    y='user_name',
                    orientation='h',
                    color='checkin_rate_period',
                    title="Top 10 Most Active Users (Period)",
                    labels={'checkin_count_period': 'Checkins', 'user_name': 'User', 'checkin_rate_period': 'Rate (%)'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(top_period, use_container_width=True)
        
        with col2:
            st.subheader("🏆 Most Active (Overall)")
            if not overall_df.empty:
                top_overall = overall_df.nlargest(10, 'total_checkins')[['user_name', 'total_checkins', 'checkin_rate', 'days_active']].round(1)
                
                # Enhanced bar chart
                fig = px.bar(
                    top_overall,
                    x='total_checkins',
                    y='user_name',
                    orientation='h',
                    color='checkin_rate',
                    title="Top 10 Most Active Users (Overall)",
                    labels={'total_checkins': 'Total Checkins', 'user_name': 'User', 'checkin_rate': 'Rate (%)'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(top_overall, use_container_width=True)
    
    with tab3:
        # Activity patterns over time
        st.subheader("📈 Activity Patterns")
        
        # Show users with consistent activity
        consistent_users = [u for u in period_checkins if u['checkin_count_period'] > 0 and u['days_between_checkins'] > 7]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("👥 Consistent Users", len(consistent_users), help="Users with checkins spread over 7+ days")
            
            if consistent_users:
                consistency_df = pd.DataFrame(consistent_users)
                
                fig = px.scatter(
                    consistency_df,
                    x='days_between_checkins',
                    y='checkin_count_period',
                    size='checkin_rate_period',
                    hover_data=['user_name'],
                    title="Checkin Consistency: Duration vs Count",
                    labels={
                        'days_between_checkins': 'Days Between First & Last Checkin',
                        'checkin_count_period': 'Number of Checkins'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Activity level categorization
            activity_categories = []
            for user in period_checkins:
                if user['checkin_count_period'] == 0:
                    category = 'Inactive'
                elif user['checkin_count_period'] <= 2:
                    category = 'Low Activity'
                elif user['checkin_count_period'] <= 5:
                    category = 'Moderate Activity'
                else:
                    category = 'High Activity'
                activity_categories.append(category)
            
            activity_counts = pd.Series(activity_categories).value_counts()
            
            fig = px.pie(
                values=activity_counts.values,
                names=activity_counts.index,
                title="Activity Level Distribution",
                color_discrete_map={
                    'Inactive': '#E74C3C',
                    'Low Activity': '#F39C12',
                    'Moderate Activity': '#3498DB',
                    'High Activity': '#27AE60'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        # Insights and recommendations
        st.subheader("🔍 Key Insights & Recommendations")
        
        # Calculate insights
        zero_checkin_users = len([u for u in period_checkins if u['checkin_count_period'] == 0])
        single_checkin_users = len([u for u in period_checkins if u['checkin_count_period'] == 1])
        
        insights = []
        
        if zero_checkin_users > 0:
            insights.append({
                'type': 'warning',
                'title': 'Inactive Users',
                'message': f'{zero_checkin_users} users have not made any checkins this period',
                'recommendation': 'Schedule 1-on-1 meetings to understand barriers and provide support'
            })
        
        if single_checkin_users > 0:
            insights.append({
                'type': 'info',
                'title': 'Low Activity Users',
                'message': f'{single_checkin_users} users have made only 1 checkin this period',
                'recommendation': 'Send reminder notifications and provide training on checkin best practices'
            })
        
        if highly_active > 0:
            insights.append({
                'type': 'success',
                'title': 'High Performers',
                'message': f'{highly_active} users are highly active (5+ checkins)',
                'recommendation': 'Recognize these users and ask them to mentor others'
            })
        
        avg_frequency = np.mean([u['checkin_frequency_per_week'] for u in overall_checkins if u['checkin_frequency_per_week'] > 0])
        if avg_frequency < 1:
            insights.append({
                'type': 'warning',
                'title': 'Low Frequency',
                'message': f'Average checkin frequency is {avg_frequency:.1f} per week',
                'recommendation': 'Consider implementing weekly checkin reminders or mandatory minimums'
            })
        
        # Display insights
        for insight in insights:
            if insight['type'] == 'warning':
                st.warning(f"⚠️ **{insight['title']}:** {insight['message']}")
                st.info(f"💡 **Recommendation:** {insight['recommendation']}")
            elif insight['type'] == 'info':
                st.info(f"ℹ️ **{insight['title']}:** {insight['message']}")
                st.info(f"💡 **Recommendation:** {insight['recommendation']}")
            elif insight['type'] == 'success':
                st.success(f"✅ **{insight['title']}:** {insight['message']}")
                st.info(f"💡 **Recommendation:** {insight['recommendation']}")
        
        # Summary statistics
        st.subheader("📊 Summary Statistics")
        
        stats_data = {
            'Metric': [
                'Total Users Analyzed',
                'Users with Checkins (Period)',
                'Users with Checkins (Overall)',
                'Average Checkins per Active User',
                'Median Checkins per User',
                'Standard Deviation',
                'Users with 100% Checkin Rate'
            ],
            'Value': [
                len(period_checkins),
                active_users,
                len([u for u in overall_checkins if u['total_checkins'] > 0]),
                f"{np.mean([u['checkin_count_period'] for u in period_checkins if u['checkin_count_period'] > 0]):.1f}",
                f"{np.median([u['checkin_count_period'] for u in period_checkins]):.1f}",
                f"{np.std([u['checkin_count_period'] for u in period_checkins]):.1f}",
                len([u for u in period_checkins if u['checkin_rate_period'] == 100])
            ]
        }
        
        stats_df = pd.DataFrame(stats_data)
        st.dataframe(stats_df, use_container_width=True, hide_index=True)

def show_enhanced_export_options(analyzer):
    """Enhanced export options with more formats and data"""
    
    st.subheader("💾 Enhanced Export Options")
    
    # Export statistics
    export_stats = []
    
    if analyzer.final_df is not None:
        export_stats.append(("Final Dataset", len(analyzer.final_df), len(analyzer.final_df.columns)))
    if analyzer.analysis_df is not None:
        export_stats.append(("Analysis Data", len(analyzer.analysis_df), len(analyzer.analysis_df.columns)))
    if analyzer.insights_df is not None:
        export_stats.append(("Insights Data", len(analyzer.insights_df), len(analyzer.insights_df.columns)))
    if analyzer.filtered_members_df is not None:
        export_stats.append(("Members Data", len(analyzer.filtered_members_df), len(analyzer.filtered_members_df.columns)))
    
    if export_stats:
        st.info("📊 **Available Data for Export:**")
        for name, rows, cols in export_stats:
            st.text(f"• {name}: {rows} rows, {cols} columns")
    
    # Enhanced export buttons
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("📊 Export Final Dataset", use_container_width=True):
            if analyzer.final_df is not None:
                csv = analyzer.final_df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download CSV",
                    data=csv,
                    file_name=f"okr_final_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                st.success(f"✅ Ready to download: {len(analyzer.final_df)} rows")
    
    with col2:
        if st.button("📈 Export Analysis Data", use_container_width=True):
            if analyzer.analysis_df is not None:
                csv = analyzer.analysis_df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download CSV",
                    data=csv,
                    file_name=f"okr_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                st.success(f"✅ Ready to download: {len(analyzer.analysis_df)} rows")
    
    with col3:
        if st.button("🎯 Export Insights", use_container_width=True):
            if analyzer.insights_df is not None:
                csv = analyzer.insights_df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download CSV",
                    data=csv,
                    file_name=f"okr_insights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                st.success(f"✅ Ready to download: {len(analyzer.insights_df)} rows")
    
    with col4:
        if st.button("👥 Export Members", use_container_width=True):
            if analyzer.filtered_members_df is not None:
                csv = analyzer.filtered_members_df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download CSV",
                    data=csv,
                    file_name=f"filtered_members_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                st.success(f"✅ Ready to download: {len(analyzer.filtered_members_df)} rows")
    
    with col5:
        if st.button("📋 Export All Data", use_container_width=True):
            # Create a combined export with all data
            combined_data = {}
            
            if analyzer.final_df is not None:
                combined_data['final_dataset'] = analyzer.final_df.to_csv(index=False)
            if analyzer.analysis_df is not None:
                combined_data['analysis'] = analyzer.analysis_df.to_csv(index=False)
            if analyzer.insights_df is not None:
                combined_data['insights'] = analyzer.insights_df.to_csv(index=False)
            if analyzer.filtered_members_df is not None:
                combined_data['members'] = analyzer.filtered_members_df.to_csv(index=False)
            
            if combined_data:
                # Create a summary report
                summary = f"""OKR Analysis Export Summary
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Exported Files: {len(combined_data)}

Files Included:
"""
                for file_name in combined_data.keys():
                    summary += f"- {file_name}.csv\n"
                
                st.download_button(
                    label="⬇️ Download Summary",
                    data=summary,
                    file_name=f"okr_export_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                    mime="text/plain",
                    use_container_width=True
                )
                st.success(f"✅ Summary ready: {len(combined_data)} files")

# Quick analysis functions
def show_quick_missing_analysis(analyzer):
    """Quick missing analysis for sidebar"""
    st.subheader("🚨 Quick Missing Analysis")
    
    members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
    
    total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("❌ No Goals", len(members_without_goals))
        if members_without_goals:
            st.error(f"{len(members_without_goals)}/{total_members} members need goals")
    
    with col2:
        st.metric("📝 No Checkins", len(members_without_checkins))
        if members_without_checkins:
            st.warning(f"{len(members_without_checkins)}/{total_members} members need checkins")
    
    with col3:
        st.metric("⚠️ Goals but No Checkins", len(members_with_goals_no_checkins))
        if members_with_goals_no_checkins:
            st.info(f"{len(members_with_goals_no_checkins)} members have goals but no checkins")

def show_quick_okr_analysis(analyzer, selected_cycle):
    """Quick OKR analysis for sidebar"""
    st.subheader("🎯 Quick OKR Analysis")
    
    okr_shifts = analyzer.get_okr_shifts_from_insights()
    
    if okr_shifts:
        progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0])
        stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0])
        issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0])
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("📈 Progress", progress_users, delta=f"{progress_users/len(okr_shifts)*100:.0f}%")
        
        with col2:
            st.metric("➡️ Stable", stable_users, delta=f"{stable_users/len(okr_shifts)*100:.0f}%")
        
        with col3:
            st.metric("📉 Issues", issue_users, delta=f"{issue_users/len(okr_shifts)*100:.0f}%")
        
        # Quick chart
        if len(okr_shifts) > 0:
            top_5 = sorted(okr_shifts, key=lambda x: x['okr_shift'], reverse=True)[:5]
            
            fig = px.bar(
                x=[u['okr_shift'] for u in top_5],
                y=[u['user_name'] for u in top_5],
                orientation='h',
                title="Top 5 OKR Performers",
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No OKR data available")

def show_quick_checkin_analysis(analyzer):
    """Quick checkin analysis for sidebar"""
    st.subheader("📝 Quick Checkin Analysis")
    
    period_checkins, overall_checkins = analyzer.analyze_checkin_behavior_from_analysis()
    
    if period_checkins:
        active_users = len([u for u in period_checkins if u['checkin_count_period'] > 0])
        avg_checkins = np.mean([u['checkin_count_period'] for u in period_checkins])
        max_checkins = max([u['checkin_count_period'] for u in period_checkins])
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("🟢 Active Users", active_users, delta=f"{active_users/len(period_checkins)*100:.0f}%")
        
        with col2:
            st.metric("📊 Avg Checkins", f"{avg_checkins:.1f}")
        
        with col3:
            st.metric("🏆 Max Checkins", max_checkins)
        
        # Quick activity distribution
        activity_levels = []
        for user in period_checkins:
            if user['checkin_count_period'] == 0:
                activity_levels.append('Inactive')
            elif user['checkin_count_period'] <= 2:
                activity_levels.append('Low')
            elif user['checkin_count_period'] <= 5:
                activity_levels.append('Moderate')
            else:
                activity_levels.append('High')
        
        activity_counts = pd.Series(activity_levels).value_counts()
        
        fig = px.pie(
            values=activity_counts.values,
            names=activity_counts.index,
            title="Activity Distribution",
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No checkin data available")

def send_enhanced_email_report(analyzer, email_generator, selected_cycle, email_from, email_password, email_to):
    """Send enhanced email report with progress tracking"""
    
    st.header("📧 Sending Enhanced Email Report")
    
    # Enhanced progress tracking
    progress_container = st.container()
    with progress_container:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_progress(message, progress):
            status_text.text(message)
            progress_bar.progress(progress)
    
    try:
        # Check if data is loaded
        if analyzer.final_df is None or analyzer.final_df.empty:
            st.error("❌ No data available. Please load data from Google Sheets first.")
            return
        
        update_progress("🔍 Analyzing missing goals and checkins...", 0.1)
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        
        update_progress("🎯 Getting OKR shifts from Insights sheet...", 0.3)
        okr_shifts = analyzer.get_okr_shifts_from_insights()
        
        update_progress("📝 Analyzing checkin behavior...", 0.5)
        period_checkins, overall_checkins = analyzer.analyze_checkin_behavior_from_analysis()
        
        update_progress("📧 Creating enhanced email content...", 0.7)
        html_content = email_generator.create_email_content(
            analyzer, selected_cycle, members_without_goals, members_without_checkins,
            members_with_goals_no_checkins, okr_shifts
        )
        
        update_progress("📤 Sending email...", 0.9)
        subject = f"📊 Báo cáo tiến độ OKR & Checkin (Enhanced) - {selected_cycle['name']} - {datetime.now().strftime('%d/%m/%Y')}"
        
        success, message = email_generator.send_email_report(
            email_from, email_password, email_to, subject, html_content
        )
        
        progress_bar.progress(1.0)
        update_progress("✅ Email sent successfully!", 1.0)
        
        # Clear progress after a moment
        time.sleep(2)
        progress_container.empty()
        
        if success:
            st.success(f"✅ {message}")
            st.info(f"📧 Enhanced report sent to: {email_to}")
            st.success("🚀 Report generated using enhanced Google Sheets integration with real-time data!")
            
            # Show report statistics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📊 Users Analyzed", len(okr_shifts) if okr_shifts else 0)
            
            with col2:
                st.metric("🚨 Missing Goals", len(members_without_goals))
            
            with col3:
                st.metric("📝 Missing Checkins", len(members_without_checkins))
            
            with col4:
                progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0]) if okr_shifts else 0
                st.metric("📈 Progress Users", progress_users)
            
            # Show email preview option
            if st.checkbox("📋 Show enhanced email preview", value=False):
                st.subheader("Enhanced Email Preview")
                st.components.v1.html(html_content, height=800, scrolling=True)
        else:
            st.error(f"❌ {message}")
            
    except Exception as e:
        progress_container.empty()
        st.error(f"❌ Error sending enhanced email report: {e}")
        st.exception(e)  # Show full traceback for debugging

if __name__ == "__main__":
    main()
            if st.button("🔍 Test Connection", use_container_width=True):
                analyzer.test_connection()
        
        with col2:
            if st.button("🗑️ Clear Cache", use_container_width=True):
                analyzer.clear_cache()
        
        # Show connection info
        status_color = {
            "connected": "🟢",
            "error": "🔴", 
            "timeout": "🟡",
            "unknown": "⚪"
        }
        
        st.info(f"**Status:** {status_color.get(analyzer.connection_status, '⚪')} {analyzer.connection_status.title()}")
        
        if analyzer.last_error:
            st.error(f"**Last Error:** {analyzer.last_error}")
        
        # Data freshness info
        freshness_info = analyzer.get_data_freshness_info()
        
        if freshness_info['last_refresh']:
            st.success(f"**Last Refresh:** {freshness_info['last_refresh'].strftime('%H:%M:%S')}")
        
        if freshness_info['cache_entries'] > 0:
            st.info(f"**Cached Sheets:** {freshness_info['cache_entries']}")
            
            # Show cache ages if available
            if freshness_info['cache_ages']:
                st.text("**Cache Ages (minutes):**")
                for sheet, age in freshness_info['cache_ages'].items():
                    sheet_name = sheet.replace('sheet_', '')
                    st.text(f"• {sheet_name}: {age}m")

        # Google Sheets connection info
        st.subheader("📊 Google Sheets Info")
        st.success("✅ Apps Script URL: Connected")
        with st.expander("📋 View URL"):
            st.code(apps_script_url, language="text")

    # Main data management section
    st.subheader("🔄 Enhanced Data Management")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🚀 Refresh Google Sheets Data", type="primary", use_container_width=True):
            with st.spinner("Triggering comprehensive data refresh..."):
                success = analyzer.trigger_data_refresh()
            
            if success:
                st.balloons()
                st.success("✅ Data refresh completed! Ready to load fresh data.")
                # Auto-clear any cached data
                analyzer.data_cache.clear()
            else:
                st.error("❌ Data refresh failed. Please check Google Sheets manually.")
    
    with col2:
        if st.button("📥 Load All Sheets Data", type="secondary", use_container_width=True):
            with st.spinner("Loading all sheets data..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_progress(message, progress):
                    status_text.text(message)
                    progress_bar.progress(progress)
                
                df = analyzer.load_all_sheets_data_parallel(update_progress)
                
                progress_bar.empty()
                status_text.empty()
                
                if df is not None and not df.empty:
                    st.success("✅ All data loaded successfully!")
                    st.balloons()
                else:
                    st.error("❌ Failed to load data. Please refresh sheets first.")
    
    with col3:
        if st.button("🔄 Quick Data Sync", use_container_width=True):
            with st.spinner("Quick sync of critical sheets..."):
                # Load only essential sheets for quick analysis
                critical_sheets = ['Analysis', 'Insights', 'Members']
                success_count = 0
                
                for sheet in critical_sheets:
                    try:
                        if sheet == 'Analysis':
                            analyzer.analysis_df = analyzer.get_sheet_data_with_retry('Analysis')
                        elif sheet == 'Insights':
                            analyzer.insights_df = analyzer.get_sheet_data_with_retry('Insights')
                        elif sheet == 'Members':
                            analyzer.filtered_members_df = analyzer.get_sheet_data_with_retry('Members')
                        success_count += 1
                    except Exception as e:
                        st.warning(f"⚠️ Failed to sync {sheet}: {e}")
                
                if success_count == len(critical_sheets):
                    st.success(f"✅ Quick sync completed! {success_count}/{len(critical_sheets)} sheets loaded.")
                else:
                    st.warning(f"⚠️ Partial sync: {success_count}/{len(critical_sheets)} sheets loaded.")

    # Show data status summary
    if any([analyzer.final_df is not None, analyzer.analysis_df is not None, analyzer.insights_df is not None]):
        st.subheader("📊 Data Status Summary")
        
        status_data = []
        sheet_status = [
            ("Final Dataset", analyzer.final_df),
            ("Analysis", analyzer.analysis_df), 
            ("Insights", analyzer.insights_df),
            ("Members", analyzer.filtered_members_df),
            ("Cycles", analyzer.cycles_df),
            ("Goals", analyzer.goals_df),
            ("KRs", analyzer.krs_df),
            ("Checkins", analyzer.checkins_df)
        ]
        
        for sheet_name, df in sheet_status:
            if df is not None and not df.empty:
                status_data.append({
                    "Sheet": sheet_name,
                    "Status": "✅ Loaded",
                    "Rows": len(df),
                    "Columns": len(df.columns)
                })
            else:
                status_data.append({
                    "Sheet": sheet_name, 
                    "Status": "❌ Empty/Missing",
                    "Rows": 0,
                    "Columns": 0
                })
        
        status_df = pd.DataFrame(status_data)
        st.dataframe(status_df, use_container_width=True, hide_index=True)

    # Get cycles if data is loaded
    if analyzer.cycles_df is not None and not analyzer.cycles_df.empty:
        cycles = analyzer.get_available_cycles()
        
        if cycles:
            # Cycle selection
            with st.sidebar:
                st.subheader("📅 Cycle Selection")
                cycle_options = {f"{cycle['name']} ({cycle['formatted_start_time']})": cycle for cycle in cycles}
                selected_cycle_name = st.selectbox(
                    "Select Analysis Cycle",
                    options=list(cycle_options.keys()),
                    index=0,  # Default to first (latest) cycle
                    help="Choose the quarterly cycle for analysis"
                )
                
                selected_cycle = cycle_options[selected_cycle_name]
                
                st.success(f"🎯 **Selected:** {selected_cycle['name']}")
                st.info(f"📅 **Start:** {selected_cycle['formatted_start_time']}")
                st.code(f"Path: {selected_cycle['path']}")

            # Email configuration
            with st.sidebar:
                st.subheader("📧 Email Report Settings")
                
                # Pre-configured email settings
                email_from = "apluscorp.hr@gmail.com"
                email_password = 'mems nctq yxss gruw'  # App password
                email_to = "xnk3@apluscorp.vn"
                
                st.success("📧 Email settings pre-configured")
                st.text(f"From: {email_from}")
                st.text(f"To: {email_to}")
                
                # Option to override email recipient
                custom_email = st.text_input("Custom recipient (optional):", placeholder="email@example.com")
                if custom_email.strip():
                    email_to = custom_email.strip()
                    st.info(f"📧 Using custom recipient: {email_to}")

            # Enhanced analysis section
            if analyzer.final_df is not None and not analyzer.final_df.empty:
                st.subheader("🎯 Enhanced Analysis Options")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📈 Run Full Analysis", type="primary", use_container_width=True):
                        run_enhanced_analysis(analyzer, selected_cycle)
                
                with col2:
                    if st.button("📧 Send Enhanced Report", type="secondary", use_container_width=True):
                        send_enhanced_email_report(analyzer, email_generator, selected_cycle, email_from, email_password, email_to)
                
                # Quick analysis options
                st.subheader("⚡ Quick Analysis")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("🚨 Missing Analysis Only", use_container_width=True):
                        show_quick_missing_analysis(analyzer)
                
                with col2:
                    if st.button("🎯 OKR Shifts Only", use_container_width=True):
                        show_quick_okr_analysis(analyzer, selected_cycle)
                
                with col3:
                    if st.button("📝 Checkin Behavior Only", use_container_width=True):
                        show_quick_checkin_analysis(analyzer)
                        
            else:
                st.info("📥 Please load data from Google Sheets to enable analysis.")
                st.markdown("**💡 Quick Start:**")
                st.markdown("1. Click '🚀 Refresh Google Sheets Data' to update all data")
                st.markdown("2. Click '📥 Load All Sheets Data' to load into dashboard")
                st.markdown("3. Select a cycle and run analysis")
        else:
            st.warning("⚠️ No cycles found. Please refresh Google Sheets data.")
    else:
        st.info("📊 Please load data from Google Sheets to see available cycles.")


def run_enhanced_analysis(analyzer, selected_cycle):
    """Run enhanced analysis with better progress tracking"""
    
    st.header(f"📊 Enhanced Analysis Results for {selected_cycle['name']}")
    st.info("🔗 **Data Source:** Google Sheets (Real-time) | **Processing:** Enhanced with caching")
    
    # Create tabs for organized view
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Overview", "🚨 Missing Analysis", "🎯 OKR Shifts", "📝 Checkin Behavior", "💾 Export"])
    
    with tab1:
        show_enhanced_data_summary(analyzer)
    
    with tab2:
        with st.spinner("Analyzing missing goals and checkins..."):
            show_enhanced_missing_analysis(analyzer)
    
    with tab3:
        with st.spinner("Loading OKR shifts analysis..."):
            show_enhanced_okr_analysis(analyzer, selected_cycle)
    
    with tab4:
        with st.spinner("Analyzing checkin behavior..."):
            show_enhanced_checkin_analysis(analyzer)
    
    with tab5:
        show_enhanced_export_options(analyzer)

def show_enhanced_data_summary(analyzer):
    """Show enhanced data summary with more metrics"""
    st.subheader("📈 Enhanced Data Summary")
    
    # First row of metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_goals = analyzer.final_df['goal_id'].nunique() if analyzer.final_df is not None else 0
        st.metric("Total Goals", total_goals, help="Unique goals in the system")
    
    with col2:
        total_krs = analyzer.final_df['kr_id'].nunique() if analyzer.final_df is not None else 0
        st.metric("Total KRs", total_krs, help="Unique key results")
    
    with col3:
        total_checkins = analyzer.final_df['checkin_id'].nunique() if analyzer.final_df is not None else 0
        st.metric("Total Checkins", total_checkins, help="Unique checkin entries")
    
    with col4:
        total_users = analyzer.final_df['goal_user_name'].nunique() if analyzer.final_df is not None else 0
        st.metric("Active Users", total_users, help="Users with goals")
    
    with col5:
        total_filtered_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
        st.metric("Filtered Members", total_filtered_members, help="All eligible members")
    
    # Second row with calculated metrics
    if analyzer.insights_df is not None and not analyzer.insights_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_goal_value = analyzer.insights_df['final_goal_value'].mean()
            st.metric("Avg Goal Value", f"{avg_goal_value:.2f}", help="Average goal completion")
        
        with col2:
            avg_checkin_count = analyzer.insights_df['checkin_count'].mean()
            st.metric("Avg Checkins/User", f"{avg_checkin_count:.1f}", help="Average checkins per user")
        
        with col3:
            total_shifts = len(analyzer.insights_df)
            positive_shifts = len(analyzer.insights_df[analyzer.insights_df['final_goal_value'] > analyzer.insights_df['last_friday_final_goal_value']])
            st.metric("Progress Rate", f"{positive_shifts/total_shifts*100:.1f}%", help="% of users with positive progress")
        
        with col4:
            zero_checkins = len(analyzer.insights_df[analyzer.insights_df['checkin_count'] == 0])
            st.metric("No Checkins", f"{zero_checkins}", delta=f"{zero_checkins/total_shifts*100:.1f}%", help="Users with no checkins")
    
    # Data quality indicators
    st.subheader("🔍 Data Quality Indicators")
    
    quality_metrics = []
    
    if analyzer.final_df is not None:
        # Check for missing goal names
        missing_goal_names = analyzer.final_df['goal_name'].isna().sum()
        quality_metrics.append(("Missing Goal Names", missing_goal_names, "❌" if missing_goal_names > 0 else "✅"))
        
        # Check for missing user names  
        missing_user_names = analyzer.final_df['goal_user_name'].isna().sum()
        quality_metrics.append(("Missing User Names", missing_user_names, "❌" if missing_user_names > 0 else "✅"))
        
        # Check for goals without KRs
        goals_without_krs = len(analyzer.final_df[analyzer.final_df['kr_id'].isna()])
        quality_metrics.append(("Goals without KRs", goals_without_krs, "⚠️" if goals_without_krs > 0 else "✅"))
    
    if quality_metrics:
        quality_df = pd.DataFrame(quality_metrics, columns=["Metric", "Count", "Status"])
        st.dataframe(quality_df, use_container_width=True, hide_index=True)

def show_enhanced_missing_analysis(analyzer):
    """Enhanced missing analysis with better visualizations"""
    
    st.subheader("🚨 Enhanced Missing Goals & Checkins Analysis")
    
    # Get the analysis data
    members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
    
    if not any([members_without_goals, members_without_checkins, members_with_goals_no_checkins]):
        st.success("🎉 Perfect! All members have goals and checkins!")
        return
    
    total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
    
    # Enhanced summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Filtered Members", total_members)
    
    with col2:
        no_goals_count = len(members_without_goals)
        no_goals_pct = (no_goals_count / total_members * 100) if total_members > 0 else 0
        st.metric("❌ Without Goals", no_goals_count, delta=f"-{no_goals_pct:.1f}%")
    
    with col3:
        no_checkins_count = len(members_without_checkins)
        no_checkins_pct = (no_checkins_count / total_members * 100) if total_members > 0 else 0
        st.metric("📝 Without Checkins", no_checkins_count, delta=f"-{no_checkins_pct:.1f}%")
    
    with col4:
        goals_no_checkins_count = len(members_with_goals_no_checkins)
        goals_no_checkins_pct = (goals_no_checkins_count / total_members * 100) if total_members > 0 else 0
        st.metric("⚠️ Goals but No Checkins", goals_no_checkins_count, delta=f"-{goals_no_checkins_pct:.1f}%")
    
    # Enhanced visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Goal status sunburst chart
        members_with_goals = total_members - no_goals_count
        
        fig = go.Figure(data=go.Pie(
            labels=['Have Goals', 'No Goals'],
            values=[members_with_goals, no_goals_count],
            hole=0.4,
            marker_colors=['#00CC66', '#FF6B6B']
        ))
        fig.update_layout(
            title="Goal Status Distribution",
            annotations=[dict(text='Goals', x=0.5, y=0.5, font_size=20, showarrow=False)]
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Checkin status donut chart
        members_with_checkins = total_members - no_checkins_count
        
        fig = go.Figure(data=go.Pie(
            labels=['Have Checkins', 'No Checkins'],
            values=[members_with_checkins, no_checkins_count],
            hole=0.4,
            marker_colors=['#4ECDC4', '#FFE66D']
        ))
        fig.update_layout(
            title="Checkin Status Distribution",
            annotations=[dict(text='Checkins', x=0.5, y=0.5, font_size=20, showarrow=False)]
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Priority action matrix
    st.subheader("🎯 Priority Action Matrix")
    
    tab1, tab2, tab3 = st.tabs(["🚫 No Goals", "⚠️ Goals but No Checkins", "📝 No Checkins"])
    
    with tab1:
        if members_without_goals:
            st.error(f"**HIGH PRIORITY:** {len(members_without_goals)} members need to set up goals")
            no_goals_df = pd.DataFrame(members_without_goals)
            st.dataframe(
                no_goals_df[['name', 'username', 'job', 'email']],
                use_container_width=True,
                height=400
            )
        else:
            st.success("✅ All filtered members have goals!")
    
    with tab2:
        if members_with_goals_no_checkins:
            st.warning(f"**MEDIUM PRIORITY:** {len(members_with_goals_no_checkins)} members have goals but no checkins")
            goals_no_checkins_df = pd.DataFrame(members_with_goals_no_checkins)
            st.dataframe(
                goals_no_checkins_df[['name', 'username', 'job', 'email']],
                use_container_width=True,
                height=400
            )
        else:
            st.success("✅ All members with goals have made checkins!")
    
    with tab3:
        if members_without_checkins:
            st.info(f"**INFO:** {len(members_without_checkins)} members without checkins (includes those without goals)")
            no_checkins_df = pd.DataFrame(members_without_checkins)
            
            # Add color coding based on whether they have goals
            def highlight_has_goal(row):
                if row['has_goal']:
                    return ['background-color: #fff3cd'] * len(row)  # Yellow for has goals
                else:
                    return ['background-color: #f8d7da'] * len(row)  # Red for no goals
            
            styled_df = no_checkins_df[['name', 'username', 'job', 'has_goal']].style.apply(highlight_has_goal, axis=1)
            st.dataframe(styled_df, use_container_width=True, height=400)
            
            st.info("💡 **Legend:** Yellow = Has goals but no checkins, Red = No goals and no checkins")

def show_enhanced_okr_analysis(analyzer, selected_cycle):
    """Enhanced OKR analysis with more detailed insights"""
    
    st.subheader("🎯 Enhanced OKR Shift Analysis")
    
    okr_shifts = analyzer.get_okr_shifts_from_insights()
    
    if not okr_shifts:
        st.warning("⚠️ No OKR shift data available in Insights sheet")
        return
    
    # Advanced metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0])
    stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0])
    issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0])
    avg_shift = np.mean([u['okr_shift'] for u in okr_shifts])
    median_shift = np.median([u['okr_shift'] for u in okr_shifts])
    
    with col1:
        st.metric("📈 Progress Makers", progress_users, delta=f"{progress_users/len(okr_shifts)*100:.1f}%")
    
    with col2:
        st.metric("➡️ Stable Users", stable_users, delta=f"{stable_users/len(okr_shifts)*100:.1f}%")
    
    with col3:
        st.metric("📉 Issue Cases", issue_users, delta=f"{issue_users/len(okr_shifts)*100:.1f}%")
    
    with col4:
        st.metric("📊 Average Shift", f"{avg_shift:.2f}")
    
    with col5:
        st.metric("📊 Median Shift", f"{median_shift:.2f}")
    
    # Enhanced OKR shift visualization
    okr_df = pd.DataFrame(okr_shifts)
    
    # Interactive scatter plot
    fig = px.scatter(
        okr_df, 
        x='last_friday_value', 
        y='current_value',
        size='checkin_count',
        color='okr_shift',
        hover_data=['user_name', 'okr_shift', 'checkin_count'],
        color_continuous_scale=['red', 'yellow', 'green'],
        title="OKR Progress: Current vs Baseline Values",
        labels={
            'last_friday_value': 'Baseline Value (Last Friday)',
            'current_value': 'Current Value',
            'okr_shift': 'OKR Shift'
        }
    )
    
    # Add diagonal line for reference (no change)
    min_val = min(okr_df['last_friday_value'].min(), okr_df['current_value'].min())
    max_val = max(okr_df['last_friday_value'].max(), okr_df['current_value'].max())
    fig.add_shape(
        type="line",
        x0=min_val, y0=min_val,
        x1=max_val, y1=max_val,
        line=dict(color="gray", dash="dash", width=2),
    )
    
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed analysis tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🏆 Top Performers", "⚠️ Need Attention", "📊 Distribution", "📈 Trends"])
    
    with tab1:
        top_performers = okr_df[okr_df['okr_shift'] > 0].head(10)
        if not top_performers.empty:
            st.success(f"🎉 Top {len(top_performers)} performers with positive OKR shifts")
            
            # Enhanced bar chart for top performers
            fig = px.bar(
                top_performers.sort_values('okr_shift', ascending=True),
                x='okr_shift',
                y='user_name',
                orientation='h',
                color='checkin_count',
                title="Top Performers by OKR Shift",
                labels={'okr_shift': 'OKR Shift', 'user_name': 'User'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(
                top_performers[['user_name', 'okr_shift', 'current_value', 'last_friday_value', 'checkin_count']].round(2),
                use_container_width=True
            )
        else:
            st.info("No users with positive OKR shifts found")
    
    with tab2:
        issue_df = okr_df[okr_df['okr_shift'] < 0].head(10)
        if not issue_df.empty:
            st.warning(f"⚠️ {len(issue_df)} users need attention (negative OKR shifts)")
            
            # Enhanced bar chart for users needing attention
            fig = px.bar(
                issue_df.sort_values('okr_shift', ascending=False),
                x='okr_shift',
                y='user_name',
                orientation='h',
                color='checkin_count',
                color_continuous_scale='Reds',
                title="Users Needing Attention (Negative OKR Shifts)",
                labels={'okr_shift': 'OKR Shift', 'user_name': 'User'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(
                issue_df[['user_name', 'okr_shift', 'current_value', 'last_friday_value', 'checkin_count']].round(2),
                use_container_width=True
            )
        else:
            st.success("✅ No users with negative OKR shifts!")
    
    with tab3:
        # Distribution analysis
        col1, col2 = st.columns(2)
        
        with col1:
            # Histogram of OKR shifts
            fig = px.histogram(
                okr_df, 
                x='okr_shift', 
                nbins=20,
                title="Distribution of OKR Shifts",
                labels={'okr_shift': 'OKR Shift', 'count': 'Number of Users'}
            )
            fig.add_vline(x=0, line_dash="dash", line_color="red", annotation_text="No Change")
            fig.add_vline(x=avg_shift, line_dash="dot", line_color="blue", annotation_text=f"Average: {avg_shift:.2f}")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Box plot of checkin counts by shift category
            okr_df['shift_category'] = okr_df['okr_shift'].apply(
                lambda x: 'Progress' if x > 0 else 'Stable' if x == 0 else 'Issue'
            )
            
            fig = px.box(
                okr_df,
                x='shift_category',
                y='checkin_count',
                title="Checkin Count by OKR Shift Category",
                color='shift_category',
                color_discrete_map={
                    'Progress': '#27AE60',
                    'Stable': '#F39C12', 
                    'Issue': '#E74C3C'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        # Show correlation analysis
        st.subheader("📈 Correlation Analysis")
        
        correlation_data = okr_df[['okr_shift', 'current_value', 'last_friday_value', 'checkin_count']].corr()
        
        fig = px.imshow(
            correlation_data,
            text_auto=True,
            aspect="auto",
            title="Correlation Matrix of OKR Metrics",
            color_continuous_scale='RdBu'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Key insights
        checkin_okr_corr = correlation_data.loc['checkin_count', 'okr_shift']
        st.info(f"**Key Insight:** Correlation between checkin count and OKR shift: {checkin_okr_corr:.3f}")
        
        if checkin_okr_corr > 0.3:
            st.success("✅ Strong positive correlation: More checkins tend to lead to better OKR progress!")
        elif checkin_okr_corr > 0.1:
            st.info("ℹ️ Moderate positive correlation: Some relationship between checkins and progress.")
        else:
            st.warning("⚠️ Weak correlation: Checkin frequency may not directly impact OKR progress.")

def show_enhanced_checkin_analysis(analyzer):
    """Enhanced checkin behavior analysis"""
    
    st.subheader("📝 Enhanced Checkin Behavior Analysis")
    
    period_checkins, overall_checkins = analyzer.analyze_checkin_behavior_from_analysis()
    
    if not period_checkins or not overall_checkins:
        st.warning("⚠️ No checkin data available in Analysis sheet")
        return
    
    period_df = pd.DataFrame(period_checkins)
    overall_df = pd.DataFrame(overall_checkins)
    
    # Enhanced metrics
    last_friday = analyzer.get_last_friday_date()
    quarter_start = analyzer.get_quarter_start_date()
    
    st.info(f"📅 **Analysis Period:** {quarter_start.strftime('%d/%m/%Y')} - {last_friday.strftime('%d/%m/%Y')}")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    active_users = len([u for u in period_checkins if u['checkin_count_period'] > 0])
    avg_checkins = np.mean([u['checkin_count_period'] for u in period_checkins])
    max_checkins = max([u['checkin_count_period'] for u in period_checkins]) if period_checkins else 0
    avg_rate = np.mean([u['checkin_rate_period'] for u in period_checkins])
    highly_active = len([u for u in period_checkins if u['checkin_count_period'] >= 5])
    
    with col1:
        st.metric("🟢 Active Users", active_users, delta=f"{active_users/len(period_checkins)*100:.1f}%")
    
    with col2:
        st.metric("📊 Avg Checkins/User", f"{avg_checkins:.1f}")
    
    with col3:
        st.metric("🏆 Max Checkins", max_checkins)
    
    with col4:
        st.metric("📈 Avg Checkin Rate", f"{avg_rate:.1f}%")
    
    with col5:
        st.metric("🔥 Highly Active (5+)", highly_active)
    
    # Enhanced visualizations
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Distribution", "🏆 Leaderboards", "📈 Activity Patterns", "🔍 Insights"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # Checkin distribution histogram
            checkin_counts = [u['checkin_count_period'] for u in period_checkins]
            
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=checkin_counts, nbinsx=10, name="Checkin Distribution"))
            fig.update_layout(
                title="Distribution of Checkins per User (Period)",
                xaxis_title="Number of Checkins",
                yaxis_title="Number of Users",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Checkin rate distribution
            checkin_rates = [u['checkin_rate_period'] for u in period_checkins]
            
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=checkin_rates, nbinsx=10, name="Checkin Rate Distribution"))
            fig.update_layout(
                title="Distribution of Checkin Rates (Period)",
                xaxis_title="Checkin Rate (%)",
                yaxis_title="Number of Users",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:

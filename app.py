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
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
import base64
from io import BytesIO
import plotly.io as pio

warnings.filterwarnings('ignore')

# Set page config
st.set_page_config(
    page_title="OKR & Checkin Analysis",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

class OKRAnalysisSystem:
    """OKR Analysis System for Streamlit - Google Sheets Integration"""

    def __init__(self, apps_script_url: str):
        self.apps_script_url = apps_script_url
        self.final_df = None
        self.filtered_members_df = None
        self.cycles_df = None
        self.goals_df = None
        self.krs_df = None
        self.checkins_df = None
        self.analysis_df = None
        self.insights_df = None

    def fetch_sheet_data(self, sheet_name: str) -> pd.DataFrame:
        """Fetch data from specific Google Sheet via Apps Script"""
        try:
            # Call Apps Script endpoint with sheet parameter
            params = {
                'action': 'getSheetData',
                'sheet': sheet_name
            }
            
            response = requests.get(self.apps_script_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'error' in data:
                st.error(f"Apps Script Error for {sheet_name}: {data['error']}")
                return pd.DataFrame()
            
            # Check if data exists
            if 'data' not in data or not data['data']:
                st.warning(f"No data found for sheet: {sheet_name}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data['data'][1:], columns=data['data'][0])  # First row is headers
            
            return df
            
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching {sheet_name} data: {e}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Unexpected error fetching {sheet_name}: {e}")
            return pd.DataFrame()

    def refresh_google_sheets_data(self) -> bool:
        """Trigger data refresh in Google Sheets via Apps Script"""
        try:
            # Call Apps Script endpoint to refresh data
            params = {
                'action': 'refreshData'
            }
            
            response = requests.get(self.apps_script_url, params=params, timeout=120)  # Longer timeout for refresh
            response.raise_for_status()
            
            data = response.json()
            
            if 'error' in data:
                st.error(f"Apps Script Refresh Error: {data['error']}")
                return False
            
            if data.get('success'):
                st.success("✅ Google Sheets data refreshed successfully!")
                return True
            else:
                st.warning("⚠️ Data refresh completed but status unclear")
                return True
                
        except requests.exceptions.RequestException as e:
            st.error(f"Error refreshing data: {e}")
            return False
        except Exception as e:
            st.error(f"Unexpected error during refresh: {e}")
            return False

    def get_cycles_list(self) -> List[Dict]:
        """Get list of cycles from Google Sheets"""
        try:
            cycles_df = self.fetch_sheet_data('Cycles')
            if cycles_df.empty:
                return []
            
            cycles_list = []
            for _, row in cycles_df.iterrows():
                cycles_list.append({
                    'name': row.get('name', ''),
                    'path': row.get('path', ''),
                    'formatted_start_time': row.get('formatted_start_time', ''),
                    'start_time': row.get('start_time', '')
                })
            
            return cycles_list
            
        except Exception as e:
            st.error(f"Error processing cycles data: {e}")
            return []

    def get_filtered_members(self) -> pd.DataFrame:
        """Get filtered members from Google Sheets"""
        try:
            members_df = self.fetch_sheet_data('Members')
            self.filtered_members_df = members_df
            return members_df
            
        except Exception as e:
            st.error(f"Error getting filtered members: {e}")
            return pd.DataFrame()

    def load_all_sheets_data(self, progress_callback=None):
        """Load all data from Google Sheets"""
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
            
            for i, (sheet_name, attr_name) in enumerate(sheets_to_load):
                if progress_callback:
                    progress = (i + 1) / total_sheets
                    progress_callback(f"Loading {sheet_name}...", progress)
                
                df = self.fetch_sheet_data(sheet_name)
                setattr(self, attr_name, df)
                
                # Clean and convert data types for specific sheets
                if sheet_name == 'Final_Dataset' and not df.empty:
                    self._clean_final_data()
                elif sheet_name == 'Analysis' and not df.empty:
                    self._clean_analysis_data()
                elif sheet_name == 'Insights' and not df.empty:
                    self._clean_insights_data()
            
            if progress_callback:
                progress_callback("Data loading completed!", 1.0)
            
            return self.final_df
            
        except Exception as e:
            st.error(f"Error loading sheets data: {e}")
            return None

    def _clean_final_data(self):
        """Clean final dataset"""
        try:
            if self.final_df is not None and not self.final_df.empty:
                # Convert numeric columns
                numeric_columns = ['goal_current_value', 'kr_current_value', 'checkin_kr_current_value']
                for col in numeric_columns:
                    if col in self.final_df.columns:
                        self.final_df[col] = pd.to_numeric(self.final_df[col], errors='coerce').fillna(0)
                
                # Handle empty strings
                self.final_df = self.final_df.replace('', np.nan)
                
        except Exception as e:
            st.warning(f"Error cleaning final data: {e}")

    def _clean_analysis_data(self):
        """Clean analysis dataset"""
        try:
            if self.analysis_df is not None and not self.analysis_df.empty:
                # Convert numeric columns
                numeric_columns = ['goal_current_value', 'kr_current_value', 'checkin_kr_current_value', 'last_friday_checkin_value']
                for col in numeric_columns:
                    if col in self.analysis_df.columns:
                        self.analysis_df[col] = pd.to_numeric(self.analysis_df[col], errors='coerce').fillna(0)
                
                # Handle empty strings
                self.analysis_df = self.analysis_df.replace('', np.nan)
                
        except Exception as e:
            st.warning(f"Error cleaning analysis data: {e}")

    def _clean_insights_data(self):
        """Clean insights dataset"""
        try:
            if self.insights_df is not None and not self.insights_df.empty:
                # Convert numeric columns
                numeric_columns = ['final_goal_value', 'last_friday_final_goal_value', 'checkin_count']
                for col in numeric_columns:
                    if col in self.insights_df.columns:
                        self.insights_df[col] = pd.to_numeric(self.insights_df[col], errors='coerce').fillna(0)
                
        except Exception as e:
            st.warning(f"Error cleaning insights data: {e}")

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
        """Analyze members without goals and without checkins using Google Sheets data"""
        try:
            if self.filtered_members_df is None or self.filtered_members_df.empty:
                return [], [], []
            
            if self.final_df is None or self.final_df.empty:
                return [], [], []

            # Get users with goals from final dataset
            users_with_goals = set()
            if 'goal_user_name' in self.final_df.columns:
                users_with_goals = set(self.final_df['goal_user_name'].dropna().unique())
            
            # Get users with checkins from final dataset  
            users_with_checkins = set()
            if 'checkin_user_id' in self.final_df.columns and 'goal_user_id' in self.final_df.columns:
                # Create mapping from user_id to user_name using members data
                user_id_to_name = {}
                if 'id' in self.filtered_members_df.columns and 'name' in self.filtered_members_df.columns:
                    user_id_to_name = dict(zip(self.filtered_members_df['id'], self.filtered_members_df['name']))
                
                # Get checkin user IDs and map to names
                checkin_user_ids = self.final_df['checkin_user_id'].dropna().unique()
                users_with_checkins = {user_id_to_name.get(str(uid), str(uid)) for uid in checkin_user_ids if str(uid) in user_id_to_name}
            
            # Alternative: check if checkin_name exists (simpler approach)
            if 'checkin_name' in self.final_df.columns and 'goal_user_name' in self.final_df.columns:
                checkin_data = self.final_df[
                    (self.final_df['checkin_name'].notna()) & 
                    (self.final_df['checkin_name'] != '') &
                    (self.final_df['goal_user_name'].notna())
                ]
                users_with_checkins.update(checkin_data['goal_user_name'].unique())
            
            # Get all filtered members
            all_members = set(self.filtered_members_df['name'].unique()) if 'name' in self.filtered_members_df.columns else set()
            
            # Find missing groups
            members_without_goals = []
            members_without_checkins = []
            members_with_goals_no_checkins = []
            
            for member_name in all_members:
                member_info = self.filtered_members_df[self.filtered_members_df['name'] == member_name]
                if member_info.empty:
                    continue
                    
                member_info = member_info.iloc[0].to_dict()
                
                has_goal = member_name in users_with_goals
                has_checkin = member_name in users_with_checkins
                
                if not has_goal:
                    members_without_goals.append({
                        'name': member_name,
                        'username': member_info.get('username', ''),
                        'job': member_info.get('job', ''),
                        'email': member_info.get('email', ''),
                        'id': member_info.get('id', '')
                    })
                
                if not has_checkin:
                    members_without_checkins.append({
                        'name': member_name,
                        'username': member_info.get('username', ''),
                        'job': member_info.get('job', ''),
                        'email': member_info.get('email', ''),
                        'id': member_info.get('id', ''),
                        'has_goal': has_goal
                    })
                
                if has_goal and not has_checkin:
                    members_with_goals_no_checkins.append({
                        'name': member_name,
                        'username': member_info.get('username', ''),
                        'job': member_info.get('job', ''),
                        'email': member_info.get('email', ''),
                        'id': member_info.get('id', '')
                    })
            
            return members_without_goals, members_without_checkins, members_with_goals_no_checkins
            
        except Exception as e:
            st.error(f"Error analyzing missing goals and checkins: {e}")
            return [], [], []

    def get_okr_shifts_from_insights(self) -> List[Dict]:
        """Get OKR shifts data from Insights sheet"""
        try:
            if self.insights_df is None or self.insights_df.empty:
                st.warning("No insights data available")
                return []
            
            okr_shifts = []
            
            for _, row in self.insights_df.iterrows():
                user_name = row.get('goal_user_name', '')
                final_goal_value = float(row.get('final_goal_value', 0))
                last_friday_value = float(row.get('last_friday_final_goal_value', 0))
                checkin_count = int(row.get('checkin_count', 0))
                
                okr_shift = final_goal_value - last_friday_value
                
                okr_shifts.append({
                    'user_name': user_name,
                    'current_value': round(final_goal_value, 2),
                    'last_friday_value': round(last_friday_value, 2),
                    'okr_shift': round(okr_shift, 2),
                    'checkin_count': checkin_count,
                    'baseline_strategy': 'from_google_sheets'
                })
            
            # Sort by OKR shift descending
            return sorted(okr_shifts, key=lambda x: x['okr_shift'], reverse=True)
            
        except Exception as e:
            st.error(f"Error getting OKR shifts from insights: {e}")
            return []

    def analyze_checkin_behavior_from_analysis(self) -> Tuple[List[Dict], List[Dict]]:
        """Analyze checkin behavior using Analysis sheet data"""
        try:
            if self.analysis_df is None or self.analysis_df.empty:
                return [], []

            last_friday = self.get_last_friday_date()
            quarter_start = self.get_quarter_start_date()

            df = self.analysis_df.copy()
            
            # Convert checkin_since to datetime
            if 'checkin_since' in df.columns:
                df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')
            else:
                df['checkin_since_dt'] = pd.NaT

            # Filter period data
            mask_period = (df['checkin_since_dt'] >= quarter_start) & (df['checkin_since_dt'] <= last_friday)
            period_df = df[mask_period].copy()

            # Get all users
            all_users = []
            if 'goal_user_name' in df.columns:
                all_users = df['goal_user_name'].dropna().unique()

            # Period analysis
            period_checkins = self._analyze_period_checkins_sheets(period_df, all_users, df)
            
            # Overall analysis
            overall_checkins = self._analyze_overall_checkins_sheets(df, all_users)

            return period_checkins, overall_checkins

        except Exception as e:
            st.error(f"Error analyzing checkin behavior: {e}")
            return [], []

    def _analyze_period_checkins_sheets(self, period_df: pd.DataFrame, all_users: List[str], full_df: pd.DataFrame) -> List[Dict]:
        """Analyze checkins in the reference period using sheets data"""
        period_checkins = []

        for user in all_users:
            try:
                # Count unique checkins in period
                user_period_data = period_df[
                    (period_df['goal_user_name'] == user) &
                    (period_df['checkin_name'].notna()) &
                    (period_df['checkin_name'] != '')
                ]
                
                user_period_checkins = user_period_data['checkin_id'].nunique() if 'checkin_id' in user_period_data.columns else len(user_period_data)
                
                # Count user's KRs in period
                user_krs_in_period = period_df[period_df['goal_user_name'] == user]['kr_id'].nunique() if 'kr_id' in period_df.columns else 0
                
                checkin_rate = (user_period_checkins / user_krs_in_period * 100) if user_krs_in_period > 0 else 0

                # Get checkin dates
                user_checkin_dates = user_period_data['checkin_since_dt'].dropna()
                first_checkin_period = user_checkin_dates.min() if len(user_checkin_dates) > 0 else None
                last_checkin_period = user_checkin_dates.max() if len(user_checkin_dates) > 0 else None

                period_checkins.append({
                    'user_name': user,
                    'checkin_count_period': user_period_checkins,
                    'kr_count_period': user_krs_in_period,
                    'checkin_rate_period': checkin_rate,
                    'first_checkin_period': first_checkin_period,
                    'last_checkin_period': last_checkin_period,
                    'days_between_checkins': (last_checkin_period - first_checkin_period).days if first_checkin_period and last_checkin_period else 0
                })
                
            except Exception as e:
                st.warning(f"Error analyzing period checkins for {user}: {e}")
                continue

        return sorted(period_checkins, key=lambda x: x['checkin_count_period'], reverse=True)

    def _analyze_overall_checkins_sheets(self, df: pd.DataFrame, all_users: List[str]) -> List[Dict]:
        """Analyze overall checkin behavior using sheets data"""
        overall_checkins = []

        for user in all_users:
            try:
                # Get user's all checkins
                user_checkins_data = df[
                    (df['goal_user_name'] == user) &
                    (df['checkin_name'].notna()) &
                    (df['checkin_name'] != '')
                ]
                
                user_total_checkins = user_checkins_data['checkin_id'].nunique() if 'checkin_id' in user_checkins_data.columns else len(user_checkins_data)
                user_total_krs = df[df['goal_user_name'] == user]['kr_id'].nunique() if 'kr_id' in df.columns else 0
                
                checkin_rate = (user_total_checkins / user_total_krs * 100) if user_total_krs > 0 else 0

                # Get checkin dates
                user_checkins_dates = user_checkins_data['checkin_since_dt'].dropna()
                first_checkin = user_checkins_dates.min() if len(user_checkins_dates) > 0 else None
                last_checkin = user_checkins_dates.max() if len(user_checkins_dates) > 0 else None
                days_active = (last_checkin - first_checkin).days if first_checkin and last_checkin else 0

                checkin_frequency = (user_total_checkins / (days_active / 7)) if days_active > 0 else 0

                overall_checkins.append({
                    'user_name': user,
                    'total_checkins': user_total_checkins,
                    'total_krs': user_total_krs,
                    'checkin_rate': checkin_rate,
                    'first_checkin': first_checkin,
                    'last_checkin': last_checkin,
                    'days_active': days_active,
                    'checkin_frequency_per_week': checkin_frequency
                })
                
            except Exception as e:
                st.warning(f"Error analyzing overall checkins for {user}: {e}")
                continue

        return sorted(overall_checkins, key=lambda x: x['total_checkins'], reverse=True)


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
            
            for i, (name, value) in enumerate(list(data.items())[:15]):
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
                <p>Ngày báo cáo: {current_date} | Dữ liệu từ Google Sheets</p>
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
                <p>📊 Báo cáo được tạo tự động từ Google Sheets via Apps Script</p>
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
                    <th>📝 Checkins</th>
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
                <td><span style='color: #9b59b6;'>{item.get('checkin_count', 0)}</span></td>
            </tr>
            """
        
        html += "</tbody></table>"
        return html

    def send_email_report(self, email_from, password, email_to, subject, html_content, 
                         company_name="A Plus Mineral Material Corporation"):
        """Send email report with improved compatibility"""
        try:
            # Create message
            message = MIMEMultipart('related')
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


# ==================== STREAMLIT APP ====================

def main():
    st.title("🎯 OKR & Checkin Analysis Dashboard")
    st.markdown("### 📊 Google Sheets Integration")
    st.markdown("---")

    # Apps Script URL
    apps_script_url = "https://script.google.com/macros/s/AKfycbyW0vaFdCRKAGz3AQndFBVS_jMx0ZAzJxzusmKb4guRnc0D5-BnGCj7ujzeWdC9dhdibw/exec"

    # Initialize analyzer
    try:
        analyzer = OKRAnalysisSystem(apps_script_url)
        email_generator = EmailReportGenerator()
    except Exception as e:
        st.error(f"Failed to initialize analyzer: {e}")
        return

    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Show connection status
        st.subheader("🔗 Google Sheets Connection")
        st.success("✅ Apps Script URL: Connected")
        st.code(apps_script_url, language=None)

        # Data refresh section
        st.subheader("🔄 Data Management")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh Sheets Data", help="Trigger Google Sheets to fetch fresh data from APIs"):
                with st.spinner("Refreshing Google Sheets data..."):
                    success = analyzer.refresh_google_sheets_data()
                    if success:
                        st.rerun()  # Reload the app to show fresh data
        
        with col2:
            if st.button("📊 Load Current Data", help="Load data from current Google Sheets"):
                st.rerun()

    # Get cycles from Google Sheets
    with st.spinner("🔄 Loading cycles from Google Sheets..."):
        cycles = analyzer.get_cycles_list()

    if not cycles:
        st.error("❌ Could not load cycles from Google Sheets. Please check the connection and ensure data is available.")
        
        # Show troubleshooting info
        with st.expander("🔧 Troubleshooting"):
            st.markdown("""
            **Possible issues:**
            1. Google Sheets data hasn't been populated yet
            2. Apps Script endpoint is not responding
            3. Network connectivity issues
            
            **Solutions:**
            1. Click "Refresh Sheets Data" to fetch fresh data
            2. Check if the Google Apps Script is running properly
            3. Ensure the Google Sheets contains data in the 'Cycles' sheet
            """)
        return

    # Cycle selection
    with st.sidebar:
        st.subheader("📅 Cycle Selection")
        if cycles:
            cycle_options = {f"{cycle['name']} ({cycle['formatted_start_time']})": cycle for cycle in cycles}
            selected_cycle_name = st.selectbox(
                "Select Cycle",
                options=list(cycle_options.keys()),
                index=0,
                help="Choose the quarterly cycle to analyze"
            )
            
            selected_cycle = cycle_options[selected_cycle_name]
            
            st.info(f"🎯 **Selected Cycle:**\n\n**{selected_cycle['name']}**\n\nPath: `{selected_cycle['path']}`\n\nStart: {selected_cycle['formatted_start_time']}")
        else:
            st.error("No cycles available")
            selected_cycle = None

    # Analysis options
    with st.sidebar:
        st.subheader("📊 Analysis Options")
        show_missing_analysis = st.checkbox("Show Missing Goals & Checkins Analysis", value=True)
        show_raw_data = st.checkbox("Show Raw Data Tables", value=False)

    # Email configuration
    with st.sidebar:
        st.subheader("📧 Email Report Settings")
        
        # Pre-configured email settings
        email_from = "apluscorp.hr@gmail.com"
        email_password = 'mems nctq yxss gruw'
        email_to = "xnk3@apluscorp.vn"
        
        st.info("📧 Email settings are pre-configured")
        st.text(f"From: {email_from}")
        st.text(f"To: {email_to}")
        
        # Option to override email recipient
        custom_email = st.text_input("Custom recipient (optional):", placeholder="email@example.com")
        if custom_email.strip():
            email_to = custom_email.strip()

    # Main analysis
    if selected_cycle:
        col1, col2 = st.columns(2)
        
        with col1:
            analyze_button = st.button("🚀 Start Analysis", type="primary", use_container_width=True)
        
        with col2:
            email_button = st.button("📧 Send Email Report", type="secondary", use_container_width=True)

        if analyze_button:
            run_analysis_from_sheets(analyzer, selected_cycle, show_missing_analysis, show_raw_data)

        if email_button:
            send_email_report_from_sheets(analyzer, email_generator, selected_cycle, email_from, email_password, email_to)

def send_email_report_from_sheets(analyzer, email_generator, selected_cycle, email_from, email_password, email_to):
    """Send email report using Google Sheets data"""
    
    st.header("📧 Sending Email Report")
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(message, progress):
        status_text.text(message)
        progress_bar.progress(progress)
    
    try:
        # Load data from Google Sheets
        update_progress("Loading data from Google Sheets...", 0.2)
        df = analyzer.load_all_sheets_data(update_progress)
        
        if df is None or (hasattr(df, 'empty') and df.empty):
            st.error("❌ Failed to load data from Google Sheets for email report")
            return
        
        update_progress("Analyzing missing goals and checkins...", 0.6)
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        
        update_progress("Getting OKR shifts from Insights sheet...", 0.7)
        okr_shifts = analyzer.get_okr_shifts_from_insights()
        
        update_progress("Creating email content...", 0.8)
        html_content = email_generator.create_email_content(
            analyzer, selected_cycle, members_without_goals, members_without_checkins,
            members_with_goals_no_checkins, okr_shifts
        )
        
        update_progress("Sending email...", 0.9)
        subject = f"📊 Báo cáo tiến độ OKR & Checkin - {selected_cycle['name']} - {datetime.now().strftime('%d/%m/%Y')}"
        
        success, message = email_generator.send_email_report(
            email_from, email_password, email_to, subject, html_content
        )
        
        progress_bar.empty()
        status_text.empty()
        
        if success:
            st.success(f"✅ {message}")
            st.info(f"📧 Email report sent to: {email_to}")
            
            # Show email preview
            if st.checkbox("📋 Show email preview", value=False):
                st.subheader("Email Preview")
                st.components.v1.html(html_content, height=800, scrolling=True)
        else:
            st.error(f"❌ {message}")
            
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"❌ Error sending email report: {e}")

def run_analysis_from_sheets(analyzer, selected_cycle, show_missing_analysis, show_raw_data):
    """Run analysis using Google Sheets data"""
    
    st.header(f"📊 Analysis Results for {selected_cycle['name']}")
    st.info("🔗 **Data Source:** Google Sheets via Apps Script")
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(message, progress):
        status_text.text(message)
        progress_bar.progress(progress)
    
    # Load all data from sheets
    try:
        df = analyzer.load_all_sheets_data(update_progress)
        
        if df is None or (hasattr(df, 'empty') and df.empty):
            st.error("❌ Failed to load data from Google Sheets. Please check the connection and data availability.")
            return
            
        progress_bar.empty()
        status_text.empty()
        
        # Show data summary
        show_data_summary_from_sheets(analyzer)
        
        # Show missing goals and checkins analysis if enabled
        if show_missing_analysis:
            st.subheader("🚨 Missing Goals & Checkins Analysis")
            with st.spinner("Analyzing missing goals and checkins..."):
                show_missing_analysis_from_sheets(analyzer)
        
        # Show OKR shifts from Insights sheet
        st.subheader("🎯 OKR Shift Analysis (from Insights Sheet)")
        with st.spinner("Loading OKR shifts from Insights sheet..."):
            okr_shifts = analyzer.get_okr_shifts_from_insights()
        
        if okr_shifts:
            show_okr_analysis_from_sheets(okr_shifts, analyzer.get_last_friday_date())
        else:
            st.warning("No OKR shift data available from Insights sheet")
        
        # Analyze checkin behavior from Analysis sheet
        st.subheader("📝 Checkin Behavior Analysis (from Analysis Sheet)")
        with st.spinner("Analyzing checkin behavior..."):
            period_checkins, overall_checkins = analyzer.analyze_checkin_behavior_from_analysis()
        
        if period_checkins and overall_checkins:
            show_checkin_analysis_from_sheets(period_checkins, overall_checkins, analyzer.get_last_friday_date(), analyzer.get_quarter_start_date())
        else:
            st.warning("No checkin data available from Analysis sheet")
        
        # Show raw data if enabled
        if show_raw_data:
            show_raw_data_tables(analyzer)
        
        # Data export
        st.subheader("💾 Export Data")
        show_export_options_from_sheets(analyzer, okr_shifts, period_checkins, overall_checkins)
        
        st.success("✅ Analysis completed successfully!")
        
    except Exception as e:
        st.error(f"❌ Analysis failed: {e}")
        progress_bar.empty()
        status_text.empty()

def show_data_summary_from_sheets(analyzer):
    """Show data summary from Google Sheets"""
    st.subheader("📈 Data Summary (from Google Sheets)")
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    # Get metrics from different sheets
    total_goals = 0
    total_krs = 0 
    total_checkins = 0
    total_users = 0
    total_filtered_members = 0
    total_insights = 0
    
    if analyzer.goals_df is not None and not analyzer.goals_df.empty:
        total_goals = len(analyzer.goals_df)
    
    if analyzer.krs_df is not None and not analyzer.krs_df.empty:
        total_krs = len(analyzer.krs_df)
    
    if analyzer.checkins_df is not None and not analyzer.checkins_df.empty:
        total_checkins = len(analyzer.checkins_df)
    
    if analyzer.final_df is not None and not analyzer.final_df.empty and 'goal_user_name' in analyzer.final_df.columns:
        total_users = analyzer.final_df['goal_user_name'].nunique()
    
    if analyzer.filtered_members_df is not None and not analyzer.filtered_members_df.empty:
        total_filtered_members = len(analyzer.filtered_members_df)
    
    if analyzer.insights_df is not None and not analyzer.insights_df.empty:
        total_insights = len(analyzer.insights_df)
    
    with col1:
        st.metric("Total Goals", total_goals)
    
    with col2:
        st.metric("Total KRs", total_krs)
    
    with col3:
        st.metric("Total Checkins", total_checkins)
    
    with col4:
        st.metric("Active Users", total_users)
    
    with col5:
        st.metric("Filtered Members", total_filtered_members)
    
    with col6:
        st.metric("Insights Records", total_insights)

def show_missing_analysis_from_sheets(analyzer):
    """Show missing analysis using Google Sheets data"""
    
    # Get the analysis data
    members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
    
    with col1:
        st.metric("Total Filtered Members", total_members)
    
    with col2:
        no_goals_count = len(members_without_goals)
        no_goals_pct = (no_goals_count / total_members * 100) if total_members > 0 else 0
        st.metric("Members Without Goals", no_goals_count, delta=f"{no_goals_pct:.1f}%")
    
    with col3:
        no_checkins_count = len(members_without_checkins)
        no_checkins_pct = (no_checkins_count / total_members * 100) if total_members > 0 else 0
        st.metric("Members Without Checkins", no_checkins_count, delta=f"{no_checkins_pct:.1f}%")
    
    with col4:
        goals_no_checkins_count = len(members_with_goals_no_checkins)
        goals_no_checkins_pct = (goals_no_checkins_count / total_members * 100) if total_members > 0 else 0
        st.metric("Has Goals but No Checkins", goals_no_checkins_count, delta=f"{goals_no_checkins_pct:.1f}%")
    
    # Visual representation
    st.subheader("📊 Missing Analysis Visualization")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Goals pie chart
        members_with_goals = total_members - no_goals_count
        goal_data = pd.DataFrame({
            'Status': ['Have Goals', 'No Goals'],
            'Count': [members_with_goals, no_goals_count]
        })
        
        fig_goals = px.pie(
            goal_data, 
            values='Count', 
            names='Status',
            title="Goal Status Distribution",
            color_discrete_map={'Have Goals': '#00CC66', 'No Goals': '#FF6B6B'}
        )
        st.plotly_chart(fig_goals, use_container_width=True)
        
        # Members Without Goals table
        st.subheader("🚫 Members Without Goals")
        if members_without_goals:
            no_goals_df = pd.DataFrame(members_without_goals)
            st.dataframe(
                no_goals_df[['name', 'username', 'job', 'email']],
                use_container_width=True,
                height=300
            )
            
            # Download button
            csv_no_goals = no_goals_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Members Without Goals",
                data=csv_no_goals,
                file_name=f"members_without_goals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="download_no_goals"
            )
        else:
            st.success("✅ All filtered members have goals!")
    
    with col2:
        # Checkins pie chart  
        members_with_checkins = total_members - no_checkins_count
        checkin_data = pd.DataFrame({
            'Status': ['Have Checkins', 'No Checkins'],
            'Count': [members_with_checkins, no_checkins_count]
        })
        
        fig_checkins = px.pie(
            checkin_data, 
            values='Count', 
            names='Status',
            title="Checkin Status Distribution",
            color_discrete_map={'Have Checkins': '#4ECDC4', 'No Checkins': '#FFE66D'}
        )
        st.plotly_chart(fig_checkins, use_container_width=True)
        
        # Members with Goals but No Checkins table
        if members_with_goals_no_checkins:
            st.subheader("⚠️ Members with Goals but No Checkins")
            st.warning("These members have set up goals but haven't made any checkins yet.")
            
            goals_no_checkins_df = pd.DataFrame(members_with_goals_no_checkins)
            st.dataframe(
                goals_no_checkins_df[['name', 'username', 'job', 'email']],
                use_container_width=True,
                height=300
            )
            
            # Download button
            csv_goals_no_checkins = goals_no_checkins_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Members with Goals but No Checkins",
                data=csv_goals_no_checkins,
                file_name=f"members_goals_no_checkins_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="download_goals_no_checkins"
            )
        else:
            st.success("✅ All members with goals have made checkins!")

def show_okr_analysis_from_sheets(okr_shifts, last_friday):
    """Show OKR shift analysis from Insights sheet"""
    
    st.info("📊 **Data Source:** Insights sheet from Google Sheets (pre-calculated)")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0])
    stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0])
    issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0])
    avg_shift = np.mean([u['okr_shift'] for u in okr_shifts])
    
    with col1:
        st.metric("Progress Makers", progress_users, delta=f"{progress_users/len(okr_shifts)*100:.1f}%")
    
    with col2:
        st.metric("Stable Users", stable_users, delta=f"{stable_users/len(okr_shifts)*100:.1f}%")
    
    with col3:
        st.metric("Issue Cases", issue_users, delta=f"{issue_users/len(okr_shifts)*100:.1f}%")
    
    with col4:
        st.metric("Average Shift", f"{avg_shift:.2f}")
    
    # Status indicators
    if issue_users > 0:
        st.success(f"✅ **Negative shifts detected!** Baseline calculation is working correctly.")
    elif stable_users == len(okr_shifts):
        st.warning("⚠️ All users have zero shift. Check if baseline calculation is working.")
    
    # OKR shift chart
    okr_df = pd.DataFrame(okr_shifts)
    
    fig = px.bar(
        okr_df.head(20), 
        x='user_name', 
        y='okr_shift',
        title=f"OKR Shifts by User (from Insights Sheet)",
        color='okr_shift',
        color_continuous_scale=['red', 'yellow', 'green'],
        hover_data=['current_value', 'last_friday_value', 'checkin_count']
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Data tables
    col1, col2 = st.columns(2)
    
    with col1:
        # Top performers table
        st.subheader("🏆 Top Performers")
        top_performers = okr_df[okr_df['okr_shift'] > 0].head(10)
        if not top_performers.empty:
            st.dataframe(
                top_performers[['user_name', 'okr_shift', 'current_value', 'last_friday_value', 'checkin_count']].round(2),
                use_container_width=True
            )
        else:
            st.info("No users with positive OKR shifts found")
    
    with col2:
        # Issues table
        if issue_users > 0:
            st.subheader("⚠️ Users with Issues")
            issue_df = okr_df[okr_df['okr_shift'] < 0].head(10)
            st.dataframe(
                issue_df[['user_name', 'okr_shift', 'current_value', 'last_friday_value', 'checkin_count']].round(2),
                use_container_width=True
            )
        else:
            st.subheader("😊 No Issues Found")
            st.success("All users have positive or stable OKR progress!")

def show_checkin_analysis_from_sheets(period_checkins, overall_checkins, last_friday, quarter_start):
    """Show checkin behavior analysis from Analysis sheet"""
    
    st.info("📊 **Data Source:** Analysis sheet from Google Sheets")
    
    period_df = pd.DataFrame(period_checkins)
    overall_df = pd.DataFrame(overall_checkins)
    
    # Period analysis metrics
    st.subheader(f"📅 Period Analysis ({quarter_start.strftime('%d/%m/%Y')} - {last_friday.strftime('%d/%m/%Y')})")
    
    col1, col2, col3, col4 = st.columns(4)
    
    active_users = len([u for u in period_checkins if u['checkin_count_period'] > 0])
    avg_checkins = np.mean([u['checkin_count_period'] for u in period_checkins])
    max_checkins = max([u['checkin_count_period'] for u in period_checkins]) if period_checkins else 0
    avg_rate = np.mean([u['checkin_rate_period'] for u in period_checkins])
    
    with col1:
        st.metric("Active Users", active_users, delta=f"{active_users/len(period_checkins)*100:.1f}%" if period_checkins else "0%")
    
    with col2:
        st.metric("Avg Checkins/User", f"{avg_checkins:.1f}")
    
    with col3:
        st.metric("Max Checkins", max_checkins)
    
    with col4:
        st.metric("Avg Checkin Rate", f"{avg_rate:.1f}%")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Checkin distribution chart
        if period_checkins:
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
        # Top checkin users pie chart
        if overall_checkins:
            top_5_overall = overall_df.head(5)
            
            fig = px.pie(
                values=top_5_overall['total_checkins'],
                names=top_5_overall['user_name'],
                title="Top 5 Most Active Users (Overall)"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Data tables
    col1, col2 = st.columns(2)
    
    with col1:
        # Most active in period
        st.subheader("🏆 Most Active (Period)")
        if not period_df.empty:
            top_period = period_df.nlargest(10, 'checkin_count_period')[['user_name', 'checkin_count_period', 'checkin_rate_period']].round(1)
            st.dataframe(top_period, use_container_width=True)
    
    with col2:
        # Most active overall
        st.subheader("🏆 Most Active (Overall)")
        if not overall_df.empty:
            top_overall = overall_df.nlargest(10, 'total_checkins')[['user_name', 'total_checkins', 'checkin_rate']].round(1)
            st.dataframe(top_overall, use_container_width=True)

def show_raw_data_tables(analyzer):
    """Show raw data tables from Google Sheets"""
    st.subheader("📋 Raw Data Tables")
    
    tabs = st.tabs(["🔄 Cycles", "👥 Members", "🎯 Goals", "📊 KRs", "📝 Checkins", "📈 Final Dataset", "🔍 Analysis", "📊 Insights"])
    
    with tabs[0]:  # Cycles
        st.subheader("Cycles Data")
        if analyzer.cycles_df is not None and not analyzer.cycles_df.empty:
            st.dataframe(analyzer.cycles_df, use_container_width=True)
        else:
            st.info("No cycles data available")
    
    with tabs[1]:  # Members
        st.subheader("Filtered Members Data")
        if analyzer.filtered_members_df is not None and not analyzer.filtered_members_df.empty:
            st.dataframe(analyzer.filtered_members_df, use_container_width=True)
        else:
            st.info("No members data available")
    
    with tabs[2]:  # Goals
        st.subheader("Goals Data")
        if analyzer.goals_df is not None and not analyzer.goals_df.empty:
            st.dataframe(analyzer.goals_df, use_container_width=True)
        else:
            st.info("No goals data available")
    
    with tabs[3]:  # KRs
        st.subheader("KRs Data")
        if analyzer.krs_df is not None and not analyzer.krs_df.empty:
            st.dataframe(analyzer.krs_df, use_container_width=True)
        else:
            st.info("No KRs data available")
    
    with tabs[4]:  # Checkins
        st.subheader("Checkins Data")
        if analyzer.checkins_df is not None and not analyzer.checkins_df.empty:
            st.dataframe(analyzer.checkins_df, use_container_width=True)
        else:
            st.info("No checkins data available")
    
    with tabs[5]:  # Final Dataset
        st.subheader("Final Dataset")
        if analyzer.final_df is not None and not analyzer.final_df.empty:
            st.dataframe(analyzer.final_df, use_container_width=True)
        else:
            st.info("No final dataset available")
    
    with tabs[6]:  # Analysis
        st.subheader("Analysis Data (Enhanced with last_friday_checkin_value)")
        if analyzer.analysis_df is not None and not analyzer.analysis_df.empty:
            st.info("This sheet contains deduplicated data with enhanced baseline tracking")
            st.dataframe(analyzer.analysis_df, use_container_width=True)
        else:
            st.info("No analysis data available")
    
    with tabs[7]:  # Insights
        st.subheader("Insights Data (User-level Metrics)")
        if analyzer.insights_df is not None and not analyzer.insights_df.empty:
            st.info("This sheet contains pre-calculated user-level performance metrics")
            st.dataframe(analyzer.insights_df, use_container_width=True)
        else:
            st.info("No insights data available")

def show_export_options_from_sheets(analyzer, okr_shifts, period_checkins, overall_checkins):
    """Show data export options for Google Sheets data"""
    
    st.subheader("💾 Export Options")
    
    # Create columns for export buttons
    cols = st.columns(4)
    
    with cols[0]:
        if st.button("📊 Export Insights Data"):
            if analyzer.insights_df is not None and not analyzer.insights_df.empty:
                csv = analyzer.insights_df.to_csv(index=False)
                st.download_button(
                    label="Download Insights CSV",
                    data=csv,
                    file_name=f"okr_insights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with cols[1]:
        if st.button("🔍 Export Analysis Data"):
            if analyzer.analysis_df is not None and not analyzer.analysis_df.empty:
                csv = analyzer.analysis_df.to_csv(index=False)
                st.download_button(
                    label="Download Analysis CSV",
                    data=csv,
                    file_name=f"okr_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with cols[2]:
        if st.button("📈 Export Final Dataset"):
            if analyzer.final_df is not None and not analyzer.final_df.empty:
                csv = analyzer.final_df.to_csv(index=False)
                st.download_button(
                    label="Download Dataset CSV",
                    data=csv,
                    file_name=f"okr_final_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with cols[3]:
        if st.button("👥 Export Members Data"):
            if analyzer.filtered_members_df is not None and not analyzer.filtered_members_df.empty:
                csv = analyzer.filtered_members_df.to_csv(index=False)
                st.download_button(
                    label="Download Members CSV",
                    data=csv,
                    file_name=f"filtered_members_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    # Additional export options for calculated data
    cols2 = st.columns(3)
    
    with cols2[0]:
        if st.button("🎯 Export OKR Shifts"):
            if okr_shifts:
                csv = pd.DataFrame(okr_shifts).to_csv(index=False)
                st.download_button(
                    label="Download OKR Shifts CSV",
                    data=csv,
                    file_name=f"okr_shifts_calculated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with cols2[1]:
        if st.button("📝 Export Period Checkins"):
            if period_checkins:
                csv = pd.DataFrame(period_checkins).to_csv(index=False)
                st.download_button(
                    label="Download Period Checkins CSV",
                    data=csv,
                    file_name=f"period_checkins_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with cols2[2]:
        if st.button("📊 Export Overall Checkins"):
            if overall_checkins:
                csv = pd.DataFrame(overall_checkins).to_csv(index=False)
                st.download_button(
                    label="Download Overall Checkins CSV",
                    data=csv,
                    file_name=f"overall_checkins_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

if __name__ == "__main__":
    main()

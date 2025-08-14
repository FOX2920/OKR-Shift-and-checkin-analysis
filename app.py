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
import time

warnings.filterwarnings('ignore')

# Set page config
st.set_page_config(
    page_title="OKR & Checkin Analysis (Google Sheets)",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

class GoogleSheetsOKRAnalyzer:
    """OKR Analysis System using Google Sheets as data source"""

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

    def trigger_data_refresh(self) -> bool:
        """Trigger Google Apps Script to refresh all OKR data"""
        try:
            st.info("🔄 Triggering data refresh in Google Sheets...")
            
            # Call the Apps Script endpoint to trigger fetchAllOKRData
            response = requests.get(
                f"{self.apps_script_url}?action=fetchAllOKRData",
                timeout=300  # 5 minutes timeout for data fetching
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success':
                    st.success("✅ Data refresh completed successfully!")
                    return True
                else:
                    st.error(f"❌ Data refresh failed: {result.get('message', 'Unknown error')}")
                    return False
            else:
                st.error(f"❌ Failed to trigger data refresh: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.Timeout:
            st.warning("⏱️ Data refresh is taking longer than expected. Please check the Google Sheets manually.")
            return False
        except Exception as e:
            st.error(f"❌ Error triggering data refresh: {e}")
            return False

    def get_sheet_data(self, sheet_name: str) -> pd.DataFrame:
        """Get data from a specific sheet in Google Sheets"""
        try:
            response = requests.get(
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
                        
                        # Convert numeric columns
                        numeric_columns = [col for col in df.columns if 'value' in col.lower() or 'count' in col.lower()]
                        for col in numeric_columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    st.error(f"❌ Failed to get {sheet_name} data: {result.get('message', 'Unknown error')}")
                    return pd.DataFrame()
            else:
                st.error(f"❌ HTTP error getting {sheet_name}: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            st.error(f"❌ Error getting {sheet_name} data: {e}")
            return pd.DataFrame()

    def load_all_sheets_data(self, progress_callback=None):
        """Load data from all sheets"""
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
                    progress_callback(f"Loading {sheet_name} data...", (i + 1) / total_sheets)
                
                df = self.get_sheet_data(sheet_name)
                setattr(self, attr_name, df)
                
                if not df.empty:
                    st.success(f"✅ Loaded {sheet_name}: {len(df)} rows")
                else:
                    st.warning(f"⚠️ {sheet_name} is empty or failed to load")
            
            return self.final_df
            
        except Exception as e:
            st.error(f"❌ Error loading sheets data: {e}")
            return None

    def get_available_cycles(self) -> List[Dict]:
        """Get available cycles from the Cycles sheet"""
        try:
            if self.cycles_df is None or self.cycles_df.empty:
                cycles_df = self.get_sheet_data('Cycles')
                self.cycles_df = cycles_df
            
            if self.cycles_df.empty:
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
                    st.warning(f"Error parsing cycle {row.get('name', 'Unknown')}: {e}")
                    continue
            
            # Sort by start_time descending (latest first)
            cycles.sort(key=lambda x: x['start_time'], reverse=True)
            return cycles
            
        except Exception as e:
            st.error(f"❌ Error getting cycles: {e}")
            return []

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
        
        # Set to end of day for last Friday
        last_friday = last_friday.replace(hour=23, minute=59, second=59, microsecond=999000)
        
        return last_friday

    def get_quarter_start_date(self) -> datetime:
        """Get current quarter start date"""
        today = datetime.now()
        quarter = (today.month - 1) // 3 + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        return datetime(today.year, quarter_start_month, 1)

    def analyze_missing_goals_and_checkins(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Analyze members without goals and without checkins using sheet data"""
        try:
            if self.filtered_members_df is None or self.filtered_members_df.empty:
                st.warning("Filtered members data not available")
                return [], [], []
            
            if self.final_df is None or self.final_df.empty:
                st.warning("Final dataset not available")
                return [], [], []

            # Get users with goals
            users_with_goals = set(self.final_df['goal_user_name'].dropna().unique())
            
            # Get users with checkins
            users_with_checkins = set()
            if 'checkin_user_id' in self.final_df.columns:
                # Map user IDs to names for checkins
                user_id_to_name = dict(zip(self.filtered_members_df['id'], self.filtered_members_df['name']))
                checkin_user_ids = self.final_df['checkin_user_id'].dropna().unique()
                users_with_checkins = {user_id_to_name.get(uid, uid) for uid in checkin_user_ids if uid in user_id_to_name}
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
            
            return members_without_goals, members_without_checkins, members_with_goals_no_checkins
            
        except Exception as e:
            st.error(f"Error analyzing missing goals and checkins: {e}")
            return [], [], []

    def get_okr_shifts_from_insights(self) -> List[Dict]:
        """Get OKR shifts data from Insights sheet"""
        try:
            if self.insights_df is None or self.insights_df.empty:
                insights_df = self.get_sheet_data('Insights')
                self.insights_df = insights_df
            
            if self.insights_df.empty:
                st.warning("Insights data not available")
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
                    st.warning(f"Error processing insights row: {e}")
                    continue
            
            # Sort by OKR shift descending
            okr_shifts.sort(key=lambda x: x['okr_shift'], reverse=True)
            return okr_shifts
            
        except Exception as e:
            st.error(f"Error getting OKR shifts from insights: {e}")
            return []

    def analyze_checkin_behavior_from_analysis(self) -> Tuple[List[Dict], List[Dict]]:
        """Analyze checkin behavior using Analysis sheet data"""
        try:
            if self.analysis_df is None or self.analysis_df.empty:
                analysis_df = self.get_sheet_data('Analysis')
                self.analysis_df = analysis_df
            
            if self.analysis_df.empty:
                st.warning("Analysis data not available")
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
                kr_count_period = user_period_data['kr_id'].nunique()
                kr_count_period = max(kr_count_period, user_all_data['kr_id'].nunique())  # Use total KRs if period KRs is 0
                
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
                
                total_krs = user_data['kr_id'].nunique()
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
            
            return period_checkins, overall_checkins
            
        except Exception as e:
            st.error(f"Error analyzing checkin behavior: {e}")
            return [], []


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


# ==================== STREAMLIT APP ====================

def main():
    st.title("🎯 OKR & Checkin Analysis Dashboard (Google Sheets)")
    st.markdown("---")
    
    # Google Apps Script URL
    apps_script_url = "https://script.google.com/macros/s/AKfycbzoaN14UG9SgOZNBzTbX35a-PCJGRyHPDvZNtU-AyxTSEtENReGFT5TRs-_Ua8hbmKHRw/exec"
    
    # Initialize analyzer
    try:
        analyzer = GoogleSheetsOKRAnalyzer(apps_script_url)
        email_generator = EmailReportGenerator()
    except Exception as e:
        st.error(f"Failed to initialize analyzer: {e}")
        return

    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Show Google Sheets connection status
        st.subheader("📊 Google Sheets Connection")
        st.success("✅ Apps Script URL: Connected")
        st.code(apps_script_url, language="text")

    # Data refresh section
    st.subheader("🔄 Data Management")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🚀 Refresh Google Sheets Data", type="primary", use_container_width=True):
            with st.spinner("Triggering data refresh in Google Sheets..."):
                success = analyzer.trigger_data_refresh()
            
            if success:
                st.success("✅ Data refresh completed! You can now load the latest data.")
                # Clear any cached data
                analyzer.final_df = None
                analyzer.analysis_df = None
                analyzer.insights_df = None
            else:
                st.error("❌ Data refresh failed. Please check the Google Sheets manually.")
    
    with col2:
        if st.button("📥 Load Data from Sheets", type="secondary", use_container_width=True):
            with st.spinner("Loading data from Google Sheets..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_progress(message, progress):
                    status_text.text(message)
                    progress_bar.progress(progress)
                
                df = analyzer.load_all_sheets_data(update_progress)
                
                progress_bar.empty()
                status_text.empty()
                
                if df is not None and not df.empty:
                    st.success("✅ Data loaded successfully from Google Sheets!")
                else:
                    st.error("❌ Failed to load data. Please refresh Google Sheets first.")

    # Get cycles if data is loaded
    if analyzer.cycles_df is not None and not analyzer.cycles_df.empty:
        cycles = analyzer.get_available_cycles()
        
        if cycles:
            # Cycle selection
            with st.sidebar:
                st.subheader("📅 Cycle Selection")
                cycle_options = {f"{cycle['name']} ({cycle['formatted_start_time']})": cycle for cycle in cycles}
                selected_cycle_name = st.selectbox(
                    "Select Cycle",
                    options=list(cycle_options.keys()),
                    index=0,  # Default to first (latest) cycle
                    help="Choose the quarterly cycle to analyze"
                )
                
                selected_cycle = cycle_options[selected_cycle_name]
                
                st.info(f"🎯 **Selected Cycle:**\n\n**{selected_cycle['name']}**\n\nPath: `{selected_cycle['path']}`\n\nStart: {selected_cycle['formatted_start_time']}")

            # Email configuration
            with st.sidebar:
                st.subheader("📧 Email Report Settings")
                
                # Pre-configured email settings
                email_from = "apluscorp.hr@gmail.com"
                email_password = 'mems nctq yxss gruw'  # App password
                email_to = "xnk3@apluscorp.vn"
                
                st.info("📧 Email settings are pre-configured")
                st.text(f"From: {email_from}")
                st.text(f"To: {email_to}")
                
                # Option to override email recipient
                custom_email = st.text_input("Custom recipient (optional):", placeholder="email@example.com")
                if custom_email.strip():
                    email_to = custom_email.strip()

            # Analysis section
            if analyzer.final_df is not None and not analyzer.final_df.empty:
                st.subheader("📊 Analysis Options")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📈 Run Analysis", type="primary", use_container_width=True):
                        run_google_sheets_analysis(analyzer, selected_cycle)
                
                with col2:
                    if st.button("📧 Send Email Report", type="secondary", use_container_width=True):
                        send_google_sheets_email_report(analyzer, email_generator, selected_cycle, email_from, email_password, email_to)
            else:
                st.info("📥 Please load data from Google Sheets first to run analysis.")
        else:
            st.warning("⚠️ No cycles found. Please refresh the Google Sheets data.")
    else:
        st.info("📊 Please load data from Google Sheets to see available cycles.")

def run_google_sheets_analysis(analyzer, selected_cycle):
    """Run analysis using Google Sheets data"""
    
    st.header(f"📊 Analysis Results for {selected_cycle['name']}")
    st.info("📊 **Data Source:** Google Sheets | **Calculation:** Pre-calculated in Apps Script")
    
    try:
        # Show data summary
        show_google_sheets_data_summary(analyzer)
        
        # Show missing goals and checkins analysis
        st.subheader("🚨 Missing Goals & Checkins Analysis")
        with st.spinner("Analyzing missing goals and checkins..."):
            show_google_sheets_missing_analysis(analyzer)
        
        # Show OKR shifts from Insights sheet
        st.subheader("🎯 OKR Shift Analysis (from Insights Sheet)")
        with st.spinner("Loading OKR shifts from Google Sheets..."):
            okr_shifts = analyzer.get_okr_shifts_from_insights()
        
        if okr_shifts:
            show_google_sheets_okr_analysis(okr_shifts, analyzer.get_last_friday_date())
        else:
            st.warning("No OKR shift data available in Insights sheet")
        
        # Show checkin behavior from Analysis sheet
        st.subheader("📝 Checkin Behavior Analysis (from Analysis Sheet)")
        with st.spinner("Analyzing checkin behavior..."):
            period_checkins, overall_checkins = analyzer.analyze_checkin_behavior_from_analysis()
        
        if period_checkins and overall_checkins:
            show_google_sheets_checkin_analysis(period_checkins, overall_checkins, analyzer.get_last_friday_date(), analyzer.get_quarter_start_date())
        else:
            st.warning("No checkin data available in Analysis sheet")
        
        # Data export
        st.subheader("💾 Export Data")
        show_google_sheets_export_options(analyzer, okr_shifts, period_checkins, overall_checkins)
        
        st.success("✅ Analysis completed successfully!")
        
    except Exception as e:
        st.error(f"❌ Analysis failed: {e}")

def show_google_sheets_data_summary(analyzer):
    """Show data summary from Google Sheets"""
    st.subheader("📈 Data Summary (from Google Sheets)")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_goals = analyzer.final_df['goal_id'].nunique() if analyzer.final_df is not None else 0
        st.metric("Total Goals", total_goals)
    
    with col2:
        total_krs = analyzer.final_df['kr_id'].nunique() if analyzer.final_df is not None else 0
        st.metric("Total KRs", total_krs)
    
    with col3:
        total_checkins = analyzer.final_df['checkin_id'].nunique() if analyzer.final_df is not None else 0
        st.metric("Total Checkins", total_checkins)
    
    with col4:
        total_users = analyzer.final_df['goal_user_name'].nunique() if analyzer.final_df is not None else 0
        st.metric("Total Users", total_users)
    
    with col5:
        total_filtered_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
        st.metric("Filtered Members", total_filtered_members)

def show_google_sheets_missing_analysis(analyzer):
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
        else:
            st.success("✅ All members with goals have made checkins!")

def show_google_sheets_okr_analysis(okr_shifts, last_friday):
    """Show OKR shift analysis from Google Sheets"""
    
    st.info("📊 **OKR Shifts calculated by Google Apps Script** - Advanced baseline calculation with multiple fallback strategies")
    
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
    
    # OKR shift chart
    okr_df = pd.DataFrame(okr_shifts)
    
    fig = px.bar(
        okr_df.head(20), 
        x='user_name', 
        y='okr_shift',
        title=f"OKR Shifts by User (Reference: {last_friday.strftime('%d/%m/%Y')})",
        color='okr_shift',
        color_continuous_scale=['red', 'yellow', 'green']
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Top performers and issues tables
    col1, col2 = st.columns(2)
    
    with col1:
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
        if issue_users > 0:
            st.subheader("⚠️ Users with Issues")
            issue_df = okr_df[okr_df['okr_shift'] < 0].head(10)
            st.dataframe(
                issue_df[['user_name', 'okr_shift', 'current_value', 'last_friday_value', 'checkin_count']].round(2),
                use_container_width=True
            )

def show_google_sheets_checkin_analysis(period_checkins, overall_checkins, last_friday, quarter_start):
    """Show checkin behavior analysis from Google Sheets"""
    
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
        st.metric("Active Users", active_users, delta=f"{active_users/len(period_checkins)*100:.1f}%")
    
    with col2:
        st.metric("Avg Checkins/User", f"{avg_checkins:.1f}")
    
    with col3:
        st.metric("Max Checkins", max_checkins)
    
    with col4:
        st.metric("Avg Checkin Rate", f"{avg_rate:.1f}%")
    
    # Checkin distribution chart
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
    
    # Top checkin users
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Most Active (Period)")
        if not period_df.empty:
            top_period = period_df.nlargest(10, 'checkin_count_period')[['user_name', 'checkin_count_period']].round(1)
            st.dataframe(top_period, use_container_width=True)
    
    with col2:
        st.subheader("🏆 Most Active (Overall)")
        if not overall_df.empty:
            top_overall = overall_df.nlargest(10, 'total_checkins')[['user_name', 'total_checkins']].round(1)
            st.dataframe(top_overall, use_container_width=True)

def show_google_sheets_export_options(analyzer, okr_shifts, period_checkins, overall_checkins):
    """Show data export options for Google Sheets data"""
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("📊 Export Final Dataset"):
            if analyzer.final_df is not None:
                csv = analyzer.final_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"okr_final_dataset_gsheets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with col2:
        if st.button("📈 Export Analysis Data"):
            if analyzer.analysis_df is not None:
                csv = analyzer.analysis_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"okr_analysis_gsheets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with col3:
        if st.button("🎯 Export OKR Shifts"):
            if okr_shifts:
                csv = pd.DataFrame(okr_shifts).to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"okr_shifts_gsheets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with col4:
        if st.button("📝 Export Insights"):
            if analyzer.insights_df is not None:
                csv = analyzer.insights_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"okr_insights_gsheets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with col5:
        if st.button("👥 Export Members"):
            if analyzer.filtered_members_df is not None:
                csv = analyzer.filtered_members_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"filtered_members_gsheets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

def send_google_sheets_email_report(analyzer, email_generator, selected_cycle, email_from, email_password, email_to):
    """Send email report using Google Sheets data"""
    
    st.header("📧 Sending Email Report (Google Sheets Data)")
    
    # Progress tracking
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
        
        update_progress("Analyzing missing goals and checkins...", 0.2)
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        
        update_progress("Getting OKR shifts from Insights sheet...", 0.4)
        okr_shifts = analyzer.get_okr_shifts_from_insights()
        
        update_progress("Creating email content...", 0.6)
        html_content = email_generator.create_email_content(
            analyzer, selected_cycle, members_without_goals, members_without_checkins,
            members_with_goals_no_checkins, okr_shifts
        )
        
        update_progress("Sending email...", 0.8)
        subject = f"📊 Báo cáo tiến độ OKR & Checkin (Google Sheets) - {selected_cycle['name']} - {datetime.now().strftime('%d/%m/%Y')}"
        
        success, message = email_generator.send_email_report(
            email_from, email_password, email_to, subject, html_content
        )
        
        progress_bar.empty()
        status_text.empty()
        
        if success:
            st.success(f"✅ {message}")
            st.info(f"📧 Email report sent to: {email_to}")
            st.success("📊 Report generated using Google Sheets data with pre-calculated metrics!")
            
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

if __name__ == "__main__":
    main()

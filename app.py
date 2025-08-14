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
    page_title="OKR & Checkin Analysis (From Sheets)",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

class GoogleSheetsOKRLoader:
    """Load OKR data from Google Sheets via Web App"""

    def __init__(self, web_app_url: str):
        self.web_app_url = web_app_url.rstrip('/')
        self.export_summary = None
        self.available_sheets = []

    def test_connection(self) -> Tuple[bool, str]:
        """Test connection to Google Sheets Web App"""
        try:
            response = requests.get(f"{self.web_app_url}", timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'available_sheets' in data:
                self.available_sheets = data['available_sheets']
                return True, f"✅ Connected successfully! Found {len(self.available_sheets)} sheets"
            else:
                return False, "❌ Invalid response format from web app"
                
        except requests.exceptions.RequestException as e:
            return False, f"❌ Connection failed: {str(e)}"
        except Exception as e:
            return False, f"❌ Unexpected error: {str(e)}"

    def get_export_summary(self) -> Dict:
        """Get export summary from Google Sheets"""
        try:
            response = requests.get(f"{self.web_app_url}?action=summary", timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, list) and len(data) > 0:
                # Convert list to dictionary
                summary_dict = {}
                for item in data:
                    if 'key' in item and 'value' in item:
                        summary_dict[item['key']] = item['value']
                
                self.export_summary = summary_dict
                return summary_dict
            else:
                return {}
                
        except Exception as e:
            st.error(f"Error getting export summary: {e}")
            return {}

    def refresh_data(self) -> Tuple[bool, str]:
        """Trigger data refresh in Google Sheets"""
        try:
            st.info("🔄 Triggering data refresh in Google Sheets...")
            response = requests.get(f"{self.web_app_url}?action=refresh", timeout=300)  # 5 minutes timeout
            response.raise_for_status()
            result = response.json()
            
            if result.get('success'):
                return True, f"✅ Data refreshed successfully! Cycle: {result.get('cycle', {}).get('name', 'Unknown')}"
            else:
                return False, f"❌ Data refresh failed: {result.get('error', 'Unknown error')}"
                
        except Exception as e:
            return False, f"❌ Data refresh failed: {str(e)}"

    def get_sheet_data(self, sheet_name: str) -> pd.DataFrame:
        """Get data from specific sheet"""
        try:
            response = requests.get(f"{self.web_app_url}?sheet={sheet_name}", timeout=60)
            response.raise_for_status()
            result = response.json()
            
            if 'data' in result and isinstance(result['data'], list):
                df = pd.DataFrame(result['data'])
                
                # Convert numeric columns
                numeric_columns = ['goal_current_value', 'kr_current_value', 'checkin_kr_current_value', 
                                 'last_friday_checkin_value', 'kr_shift_last_friday', 'final_goal_value', 
                                 'final_okr_goal_shift', 'checkin_count']
                
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                # Convert date columns
                date_columns = ['goal_since', 'kr_since', 'checkin_since']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            st.error(f"Error loading sheet {sheet_name}: {e}")
            return pd.DataFrame()

    def load_all_data(self) -> Dict[str, pd.DataFrame]:
        """Load all relevant data from Google Sheets"""
        data = {}
        
        # Key sheets for analysis
        required_sheets = ['Final_Dataset', 'Members', 'Analysis', 'Insights']
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, sheet_name in enumerate(required_sheets):
            status_text.text(f"Loading {sheet_name}...")
            progress_bar.progress((i + 1) / len(required_sheets))
            
            df = self.get_sheet_data(sheet_name)
            data[sheet_name] = df
            
            if df.empty:
                st.warning(f"⚠️ Sheet '{sheet_name}' is empty or not found")
        
        progress_bar.empty()
        status_text.empty()
        
        return data


class OKRAnalysisSystem:
    """OKR Analysis System using data from Google Sheets"""

    def __init__(self, sheets_loader: GoogleSheetsOKRLoader):
        self.sheets_loader = sheets_loader
        self.final_df = None
        self.filtered_members_df = None
        self.analysis_df = None
        self.insights_df = None

    def load_data_from_sheets(self) -> bool:
        """Load all data from Google Sheets"""
        try:
            # Get export summary first
            summary = self.sheets_loader.get_export_summary()
            if not summary:
                st.warning("⚠️ No export summary found. Data might be outdated.")
            else:
                # Display summary info
                st.info(f"""
                📊 **Data Summary:**
                - Last updated: {summary.get('export_timestamp', 'Unknown')}
                - Cycle: {summary.get('selected_cycle_name', 'Unknown')}
                - Total members: {summary.get('total_members', 0)}
                - Total records: {summary.get('total_records', 0)}
                """)
            
            # Load all sheet data
            data = self.sheets_loader.load_all_data()
            
            # Assign data
            self.final_df = data.get('Final_Dataset', pd.DataFrame())
            self.filtered_members_df = data.get('Members', pd.DataFrame())
            self.analysis_df = data.get('Analysis', pd.DataFrame())
            self.insights_df = data.get('Insights', pd.DataFrame())
            
            # Validate data
            if self.final_df.empty:
                st.error("❌ Final dataset is empty")
                return False
            
            if self.filtered_members_df.empty:
                st.error("❌ Members data is empty")
                return False
            
            st.success(f"✅ Data loaded successfully! {len(self.final_df)} records in final dataset")
            return True
            
        except Exception as e:
            st.error(f"❌ Error loading data from sheets: {e}")
            return False

    def get_last_friday_date(self) -> datetime:
        """Get last Friday date"""
        summary = self.sheets_loader.export_summary or {}
        last_friday_str = summary.get('last_friday_date')
        
        if last_friday_str:
            try:
                return datetime.strptime(last_friday_str, '%d/%m/%Y')
            except:
                pass
        
        # Fallback calculation
        today = datetime.now()
        days_since_monday = today.weekday()
        monday_this_week = today - timedelta(days=days_since_monday)
        monday_last_week = monday_this_week - timedelta(days=7)
        friday_last_week = monday_last_week + timedelta(days=4)
        return friday_last_week

    def get_quarter_start_date(self) -> datetime:
        """Get current quarter start date"""
        summary = self.sheets_loader.export_summary or {}
        quarter_start_str = summary.get('quarter_start_date')
        
        if quarter_start_str:
            try:
                return datetime.strptime(quarter_start_str, '%d/%m/%Y')
            except:
                pass
        
        # Fallback calculation
        today = datetime.now()
        quarter = (today.month - 1) // 3 + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        return datetime(today.year, quarter_start_month, 1)

    def analyze_missing_goals_and_checkins(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Analyze members without goals and without checkins"""
        try:
            if self.filtered_members_df is None or self.final_df is None:
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
            
            # Get all filtered members
            all_members = set(self.filtered_members_df['name'].unique())
            
            # Find missing groups
            members_without_goals = []
            members_without_checkins = []
            members_with_goals_no_checkins = []
            
            for member_name in all_members:
                member_info = self.filtered_members_df[self.filtered_members_df['name'] == member_name].iloc[0].to_dict()
                
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

    def calculate_okr_shifts_by_user(self) -> List[Dict]:
        """Calculate OKR shifts for each user using Analysis sheet data"""
        try:
            if self.analysis_df is None or self.analysis_df.empty:
                st.warning("⚠️ Analysis data not available")
                return []
            
            # Use data from Analysis sheet which already has kr_shift_last_friday calculated
            users = self.analysis_df['goal_user_name'].dropna().unique()
            user_okr_shifts = []
            
            for user in users:
                user_data = self.analysis_df[self.analysis_df['goal_user_name'] == user].copy()
                
                # Calculate current value (average of unique goal current values)
                unique_goals = user_data.drop_duplicates(subset=['goal_name'])
                current_value = unique_goals['goal_current_value'].mean() if len(unique_goals) > 0 else 0
                
                # Calculate average kr_shift_last_friday for this user
                valid_shifts = user_data['kr_shift_last_friday'].dropna()
                okr_shift = valid_shifts.mean() if len(valid_shifts) > 0 else 0
                
                # Calculate last friday value as current_value - okr_shift
                last_friday_value = current_value - okr_shift
                
                user_okr_shifts.append({
                    'user_name': user,
                    'current_value': current_value,
                    'last_friday_value': last_friday_value,
                    'okr_shift': okr_shift,
                    'kr_details_count': len(user_data)
                })
            
            return sorted(user_okr_shifts, key=lambda x: x['okr_shift'], reverse=True)
            
        except Exception as e:
            st.error(f"Error calculating OKR shifts: {e}")
            return []

    def analyze_checkin_behavior(self) -> Tuple[List[Dict], List[Dict]]:
        """Analyze checkin behavior"""
        try:
            last_friday = self.get_last_friday_date()
            quarter_start = self.get_quarter_start_date()

            df = self.final_df.copy()
            df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')

            # Filter period data
            mask_period = (df['checkin_since_dt'] >= quarter_start) & (df['checkin_since_dt'] <= last_friday)
            period_df = df[mask_period].copy()

            # Filter all-time checkin data
            all_time_df = df[df['checkin_id'].notna()].copy()

            all_users = df['goal_user_name'].dropna().unique()

            # Period analysis
            period_checkins = self._analyze_period_checkins(period_df, all_users, df)
            overall_checkins = self._analyze_overall_checkins(all_time_df, all_users, df)

            return period_checkins, overall_checkins

        except Exception as e:
            st.error(f"Error analyzing checkin behavior: {e}")
            return [], []

    def _analyze_period_checkins(self, period_df: pd.DataFrame, all_users: List[str], full_df: pd.DataFrame) -> List[Dict]:
        """Analyze checkins in the reference period"""
        period_checkins = []

        for user in all_users:
            try:
                user_period_checkins = period_df[
                    (period_df['goal_user_name'] == user) &
                    (period_df['checkin_name'].notna()) &
                    (period_df['checkin_name'] != '')
                ]['checkin_id'].nunique()

                user_krs_in_period = period_df[period_df['goal_user_name'] == user]['kr_id'].nunique()
                checkin_rate = (user_period_checkins / user_krs_in_period * 100) if user_krs_in_period > 0 else 0

                user_checkin_dates = period_df[
                    (period_df['goal_user_name'] == user) &
                    (period_df['checkin_name'].notna()) &
                    (period_df['checkin_name'] != '')
                ]['checkin_since_dt'].dropna()

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

    def _analyze_overall_checkins(self, all_time_df: pd.DataFrame, all_users: List[str], full_df: pd.DataFrame) -> List[Dict]:
        """Analyze overall checkin behavior"""
        overall_checkins = []

        for user in all_users:
            try:
                user_total_checkins = all_time_df[all_time_df['goal_user_name'] == user]['checkin_id'].nunique()
                user_total_krs = full_df[full_df['goal_user_name'] == user]['kr_id'].nunique()
                checkin_rate = (user_total_checkins / user_total_krs * 100) if user_total_krs > 0 else 0

                user_checkins_dates = all_time_df[all_time_df['goal_user_name'] == user]['checkin_since_dt'].dropna()
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

    def create_email_content(self, analyzer, selected_cycle_info, members_without_goals, members_without_checkins, 
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
        
        # Create checkin table
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
                <h2>{selected_cycle_info}</h2>
                <p>Ngày báo cáo: {current_date} | 📊 Dữ liệu từ Google Sheets</p>
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
                <p>📊 Báo cáo được tạo tự động từ dữ liệu Google Sheets</p>
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
    st.title("🎯 OKR & Checkin Analysis Dashboard (From Sheets)")
    st.markdown("**📊 Powered by Google Sheets Data**")
    st.markdown("---")

    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        st.subheader("🌐 Google Sheets Connection")
        
        # Web App URL input
        default_url = os.getenv("GOOGLE_SHEETS_WEB_APP_URL", "")
        web_app_url = st.text_input(
            "Google Sheets Web App URL:",
            value=default_url,
            placeholder="https://script.google.com/macros/s/AKfycbwP05QqGJVSqygh1zLqVH1BbloKq_07fBVu9OobkKEwGLUK1xbe0Bn8ylrmoVHEoj0jzg/exec",
            help="Enter the web app URL from your Google Apps Script deployment"
        )
        
        if not web_app_url:
            st.error("❌ Please enter the Google Sheets Web App URL")
            st.info("""
            **How to get the Web App URL:**
            1. Open your Google Apps Script project
            2. Click Deploy > New deployment
            3. Choose "Web app" type
            4. Set Execute as: "Me"
            5. Set Access: "Anyone"
            6. Click Deploy and copy the URL
            """)
            return

    # Initialize Google Sheets loader
    try:
        sheets_loader = GoogleSheetsOKRLoader(web_app_url)
        analyzer = OKRAnalysisSystem(sheets_loader)
        email_generator = EmailReportGenerator()
    except Exception as e:
        st.error(f"Failed to initialize system: {e}")
        return

    # Test connection
    with st.sidebar:
        st.subheader("🔗 Connection Status")
        if st.button("🧪 Test Connection"):
            success, message = sheets_loader.test_connection()
            if success:
                st.success(message)
            else:
                st.error(message)

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

    # Main buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        analyze_button = st.button("📊 Load & Analyze", type="primary", use_container_width=True)
    
    with col2:
        refresh_button = st.button("🔄 Refresh Data", type="secondary", use_container_width=True)
    
    with col3:
        email_button = st.button("📧 Send Email Report", type="secondary", use_container_width=True)

    # Refresh data in Google Sheets
    if refresh_button:
        with st.spinner("🔄 Refreshing data in Google Sheets..."):
            success, message = sheets_loader.refresh_data()
            if success:
                st.success(message)
                st.info("💡 You can now click 'Load & Analyze' to use the refreshed data")
            else:
                st.error(message)

    # Main analysis
    if analyze_button:
        run_analysis(analyzer, sheets_loader)

    # Send email report
    if email_button:
        send_email_report(analyzer, email_generator, sheets_loader, email_from, email_password, email_to)

def send_email_report(analyzer, email_generator, sheets_loader, email_from, email_password, email_to):
    """Send email report with analysis results"""
    
    st.header("📧 Sending Email Report")
    
    with st.spinner("📊 Loading data for email report..."):
        # Load data first
        if not analyzer.load_data_from_sheets():
            st.error("❌ Failed to load data for email report")
            return
        
        # Get cycle info from export summary
        summary = sheets_loader.export_summary or {}
        selected_cycle_info = summary.get('selected_cycle_name', 'Unknown Cycle')
        
        # Analyze missing goals and checkins
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        
        # Calculate OKR shifts
        okr_shifts = analyzer.calculate_okr_shifts_by_user()
        
        # Create email content
        html_content = email_generator.create_email_content(
            analyzer, selected_cycle_info, members_without_goals, members_without_checkins,
            members_with_goals_no_checkins, okr_shifts
        )
        
        # Send email
        subject = f"📊 Báo cáo tiến độ OKR & Checkin - {selected_cycle_info} - {datetime.now().strftime('%d/%m/%Y')}"
        
        success, message = email_generator.send_email_report(
            email_from, email_password, email_to, subject, html_content
        )
        
        if success:
            st.success(f"✅ {message}")
            st.info(f"📧 Email report sent to: {email_to}")
            
            # Show email preview
            if st.checkbox("📋 Show email preview", value=False):
                st.subheader("Email Preview")
                st.components.v1.html(html_content, height=800, scrolling=True)
        else:
            st.error(f"❌ {message}")

def run_analysis(analyzer, sheets_loader):
    """Run the main analysis"""
    
    st.header("📊 Analysis Results from Google Sheets")
    
    # Load data from sheets
    with st.spinner("📊 Loading data from Google Sheets..."):
        if not analyzer.load_data_from_sheets():
            st.error("❌ Failed to load data from Google Sheets")
            return
    
    # Show data summary
    show_data_summary(analyzer)
    
    # Show missing goals and checkins analysis
    st.subheader("🚨 Missing Goals & Checkins Analysis")
    with st.spinner("Analyzing missing goals and checkins..."):
        show_missing_analysis_section(analyzer)
    
    # Show OKR shifts using pre-calculated data from Analysis sheet
    st.subheader("🎯 OKR Shift Analysis")
    with st.spinner("Loading OKR shifts..."):
        okr_shifts = analyzer.calculate_okr_shifts_by_user()
    
    if okr_shifts:
        show_okr_analysis(okr_shifts, analyzer.get_last_friday_date())
    else:
        st.warning("No OKR shift data available")
    
    # Show insights from pre-calculated Insights sheet
    st.subheader("📈 User Performance Insights")
    if analyzer.insights_df is not None and not analyzer.insights_df.empty:
        show_insights_analysis(analyzer.insights_df)
    else:
        st.warning("No insights data available")
    
    # Analyze checkin behavior
    st.subheader("📝 Checkin Behavior Analysis")
    with st.spinner("Analyzing checkin behavior..."):
        period_checkins, overall_checkins = analyzer.analyze_checkin_behavior()
    
    if period_checkins and overall_checkins:
        show_checkin_analysis(period_checkins, overall_checkins, analyzer.get_last_friday_date(), analyzer.get_quarter_start_date())
    else:
        st.warning("No checkin data available")
    
    # Data export
    st.subheader("💾 Export Data")
    show_export_options(analyzer)
    
    st.success("✅ Analysis completed successfully!")

def show_data_summary(analyzer):
    """Show data summary statistics"""
    st.subheader("📈 Data Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_goals = analyzer.final_df['goal_id'].nunique() if not analyzer.final_df.empty else 0
        st.metric("Total Goals", total_goals)
    
    with col2:
        total_krs = analyzer.final_df['kr_id'].nunique() if not analyzer.final_df.empty else 0
        st.metric("Total KRs", total_krs)
    
    with col3:
        total_checkins = analyzer.final_df['checkin_id'].nunique() if not analyzer.final_df.empty else 0
        st.metric("Total Checkins", total_checkins)
    
    with col4:
        total_users = analyzer.final_df['goal_user_name'].nunique() if not analyzer.final_df.empty else 0
        st.metric("Total Users", total_users)
    
    with col5:
        total_filtered_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
        st.metric("Filtered Members", total_filtered_members)

def show_missing_analysis_section(analyzer):
    """Show missing goals and checkins analysis"""
    
    # Get the analysis data
    members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
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
    
    # Visual representation with tables below each chart
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

def show_okr_analysis(okr_shifts, last_friday):
    """Show OKR shift analysis"""
    
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
        st.metric("Average Shift", f"{avg_shift:.2f}", delta=None)
    
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
    
    # Top performers table
    st.subheader("🏆 Top Performers")
    top_performers = okr_df[okr_df['okr_shift'] > 0].head(10)
    if not top_performers.empty:
        st.dataframe(
            top_performers[['user_name', 'okr_shift', 'current_value', 'last_friday_value']].round(2),
            use_container_width=True
        )
    else:
        st.info("No users with positive OKR shifts found")
    
    # Issues table
    if issue_users > 0:
        st.subheader("⚠️ Users with Issues")
        issue_df = okr_df[okr_df['okr_shift'] < 0]
        st.dataframe(
            issue_df[['user_name', 'okr_shift', 'current_value', 'last_friday_value']].round(2),
            use_container_width=True
        )

def show_insights_analysis(insights_df):
    """Show insights analysis from pre-calculated data"""
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_goal_value = insights_df['final_goal_value'].mean()
        st.metric("Avg Goal Value", f"{avg_goal_value:.2f}")
    
    with col2:
        avg_shift = insights_df['final_okr_goal_shift'].mean()
        st.metric("Avg OKR Shift", f"{avg_shift:.2f}")
    
    with col3:
        total_checkins = insights_df['checkin_count'].sum()
        st.metric("Total Checkins", total_checkins)
    
    with col4:
        active_users = len(insights_df[insights_df['checkin_count'] > 0])
        st.metric("Active Users", active_users)
    
    # Top performers chart
    top_insights = insights_df.head(15)
    
    fig = px.bar(
        top_insights,
        x='goal_user_name',
        y='final_okr_goal_shift',
        title="Top Users by Final OKR Goal Shift",
        color='final_okr_goal_shift',
        color_continuous_scale=['red', 'yellow', 'green']
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Insights table
    st.subheader("📈 User Performance Insights")
    st.dataframe(
        insights_df[['goal_user_name', 'final_goal_value', 'final_okr_goal_shift', 'checkin_count']].round(2),
        use_container_width=True
    )

def show_checkin_analysis(period_checkins, overall_checkins, last_friday, quarter_start):
    """Show checkin behavior analysis"""
    
    period_df = pd.DataFrame(period_checkins)
    overall_df = pd.DataFrame(overall_checkins)
    
    # Period analysis metrics
    st.subheader(f"📅 Period Analysis ({quarter_start.strftime('%d/%m/%Y')} - {last_friday.strftime('%d/%m/%Y')})")
    
    col1, col2, col3, col4 = st.columns(4)
    
    active_users = len([u for u in period_checkins if u['checkin_count_period'] > 0])
    avg_checkins = np.mean([u['checkin_count_period'] for u in period_checkins])
    max_checkins = max([u['checkin_count_period'] for u in period_checkins])
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
    st.subheader("🏆 Most Active (Overall)")
    top_overall = overall_df.nlargest(10, 'total_checkins')[['user_name', 'total_checkins']].round(1)
    st.dataframe(top_overall, use_container_width=True)

def show_export_options(analyzer):
    """Show data export options"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("📊 Export Final Dataset"):
            if not analyzer.final_df.empty:
                csv = analyzer.final_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"okr_final_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with col2:
        if st.button("📈 Export Analysis Data"):
            if analyzer.analysis_df is not None and not analyzer.analysis_df.empty:
                csv = analyzer.analysis_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"okr_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with col3:
        if st.button("🏆 Export Insights Data"):
            if analyzer.insights_df is not None and not analyzer.insights_df.empty:
                csv = analyzer.insights_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"okr_insights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with col4:
        if st.button("👥 Export Members Data"):
            if analyzer.filtered_members_df is not None:
                csv = analyzer.filtered_members_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"filtered_members_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

if __name__ == "__main__":
    main()

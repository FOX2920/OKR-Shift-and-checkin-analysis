import streamlit as st
import pandas as pd
import numpy as np
import requests
import json
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timezone, timedelta
from typing import Dict, List, Tuple, Optional, Any
import warnings
import os
import smtplib
from collections import defaultdict
import openpyxl
from openpyxl.styles import PatternFill, Font, Border, Side, Alignment
from openpyxl.utils import get_column_letter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
import base64
from io import BytesIO
import plotly.io as pio
import io

# Configuration
warnings.filterwarnings('ignore')

# Streamlit configuration
st.set_page_config(
    page_title="OKR & Checkin Analysis",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

class DateUtils:
    """Utility class for date calculations"""
    
    @staticmethod
    def get_last_friday_date() -> datetime:
        """Get last Friday date - always returns Friday of previous week"""
        today = datetime.now()
        current_weekday = today.weekday()
        days_to_monday_current_week = current_weekday
        monday_current_week = today - timedelta(days=days_to_monday_current_week)
        monday_previous_week = monday_current_week - timedelta(days=7)
        friday_previous_week = monday_previous_week + timedelta(days=4)
        return friday_previous_week

    @staticmethod
    def get_quarter_start_date() -> datetime:
        """Get current quarter start date"""
        today = datetime.now()
        quarter = (today.month - 1) // 3 + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        return datetime(today.year, quarter_start_month, 1)

    @staticmethod
    def convert_timestamp_to_datetime(timestamp) -> Optional[str]:
        """Convert timestamp to datetime string"""
        if timestamp is None or timestamp == '' or timestamp == 0:
            return None
        try:
            return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return None

# Define User class for OKR tracking
class User:
    def __init__(self, user_id, name, co_OKR=1, checkin=0, dich_chuyen_OKR=0, score=0):
        """Initialize a user with basic attributes."""
        self.user_id = str(user_id)
        self.name = name
        self.co_OKR = co_OKR
        self.checkin = checkin
        self.dich_chuyen_OKR = dich_chuyen_OKR
        self.score = score
        self.OKR = {month: 0 for month in range(1, 13)}  # Create OKR dict for months 1-12

    def update_okr(self, month, value):
        if 1 <= month <= 12:
            self.OKR[month] = value

    def calculate_score(self):
        """Calculate score based on criteria: check-in, OKR and OKR movement."""
        score = 0.5

        # Check-in contributes 1 point
        if self.checkin == 1:
            score += 0.5

        # Having OKR contributes 1 point
        if self.co_OKR == 1:
            score += 1

        # OKR movement score
        movement = self.dich_chuyen_OKR

        if movement < 10:
            score += 0.15
        elif 10 <= movement < 25:
            score += 0.25
        elif 26 <= movement < 30:
            score += 0.5
        elif 31 <= movement < 50:
            score += 0.75
        elif 51 <= movement < 80:
            score += 1.25
        elif 81 <= movement < 99:
            score += 1.5
        elif movement >= 100:
            score += 2.5

        self.score = round(score, 2)  # Round to 2 decimal places

    def __repr__(self):
        return (f"User(id={self.user_id}, name={self.name}, co_OKR={self.co_OKR}, "
                f"checkin={self.checkin}, dich_chuyen_OKR={self.dich_chuyen_OKR}, score={self.score}, "
                f"OKR={self.OKR})")

# Updated UserManager class with integrated monthly OKR calculation
class UserManager:
    def __init__(self, account_df, krs_df, checkin_df, cycle_df=None, final_df=None):
        """Initialize UserManager, load data from dataframes."""
        self.account_df = account_df
        self.krs_df = krs_df
        self.checkin_df = checkin_df
        self.cycle_df = cycle_df
        self.final_df = final_df  # Add final_df for monthly calculations

        # Create user_id → name mapping from account_df
        self.user_name_map = {}
        if not account_df.empty and 'id' in account_df.columns and 'name' in account_df.columns:
            for _, row in account_df.iterrows():
                self.user_name_map[str(row['id'])] = row.get('name', 'Unknown')

        # Create users list
        self.users = self.create_users()

    def create_users(self):
        """Create User list from KRs data, only for users in account."""
        users = {}
        unique_user_ids = set()

        if not self.krs_df.empty and 'user_id' in self.krs_df.columns:
            for _, kr in self.krs_df.iterrows():
                user_id = str(kr.get("user_id"))
                if user_id and user_id not in unique_user_ids and user_id in self.user_name_map:
                    name = self.user_name_map[user_id]
                    users[user_id] = User(user_id, name)
                    unique_user_ids.add(user_id)

        return users

    def update_checkins(self, start_date=None, end_date=None):
        """Check and update check-in status for each user."""
        for user in self.users.values():
            if self.has_weekly_checkins(user.user_id, start_date, end_date):
                user.checkin = 1

    def has_weekly_checkins(self, user_id, start_date=None, end_date=None):
        """Kiểm tra xem user có check-in ít nhất 3 tuần trong khoảng thời gian đã chỉ định không."""
        # Set default date range if not provided
        if start_date is None:
            start_date = get_current_quarter_start()
        if end_date is None:
            end_date = date.today()
            
        # Convert to datetime with timezone for comparison
        start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_datetime = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        
        checkins = []
        
        # Thu thập tất cả các lần check-in của user từ checkin_df
        if not self.checkin_df.empty and 'user_id' in self.checkin_df.columns and 'day' in self.checkin_df.columns:
            user_checkins = self.checkin_df[self.checkin_df['user_id'].astype(str) == str(user_id)]
            
            for _, entry in user_checkins.iterrows():
                checkin_date = datetime.fromtimestamp(float(entry.get('day')), tz=timezone.utc)
                checkins.append(checkin_date)
        
        # Lọc ra các lần check-in trong khoảng thời gian đã chỉ định
        checkins_in_range = [dt for dt in checkins if start_datetime <= dt <= end_datetime]
        
        if not checkins_in_range:
            return False  # Không có check-in nào trong khoảng thời gian -> False
        
        # Lưu số tuần có check-in
        weekly_checkins = set(dt.isocalendar()[1] for dt in checkins_in_range)
        
        # Kiểm tra xem user đã check-in ít nhất 3 tuần trong khoảng thời gian chưa
        return len(weekly_checkins) >= 3

    def should_calculate_monthly_shift(self) -> bool:
        """Check if monthly shift should be calculated (not in months 1,4,7,10)"""
        current_month = datetime.now().month
        return current_month not in [1, 4, 7, 10]

    def get_last_month_end_date(self) -> datetime:
        """Get last day of previous month"""
        today = datetime.now()
        first_day_current_month = datetime(today.year, today.month, 1)
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        last_day_previous_month = last_day_previous_month.replace(hour=23, minute=59, second=59)
        return last_day_previous_month

    def calculate_kr_shift_last_month(self, row, last_month_end):
        """Calculate kr_shift_last_month = kr_current_value - last_month_end_checkin_value"""
        try:
            kr_current_value = pd.to_numeric(row.get('kr_current_value', 0), errors='coerce')
            if pd.isna(kr_current_value):
                kr_current_value = 0
            
            kr_id = row.get('kr_id', '')
            if not kr_id or self.final_df is None:
                return kr_current_value
            
            quarter_start = get_current_quarter_start()
            reference_month_end = self.get_last_month_end_date()
            
            kr_checkins = self.final_df[
                (self.final_df['kr_id'] == kr_id) & 
                (self.final_df['checkin_id'].notna()) &
                (self.final_df['checkin_name'].notna()) &
                (self.final_df['checkin_name'] != '')
            ].copy()
            
            if not kr_checkins.empty:
                kr_checkins['checkin_since_dt'] = pd.to_datetime(kr_checkins['checkin_since'], errors='coerce')
                kr_checkins = kr_checkins[
                    (kr_checkins['checkin_since_dt'] >= quarter_start) &
                    (kr_checkins['checkin_since_dt'] <= reference_month_end)
                ]
                
                if not kr_checkins.empty:
                    latest_checkin = kr_checkins.loc[kr_checkins['checkin_since_dt'].idxmax()]
                    last_month_checkin_value = pd.to_numeric(latest_checkin.get('checkin_kr_current_value', 0), errors='coerce')
                    if pd.isna(last_month_checkin_value):
                        last_month_checkin_value = 0
                else:
                    last_month_checkin_value = 0
            else:
                last_month_checkin_value = 0
            
            kr_shift = kr_current_value - last_month_checkin_value
            return kr_shift
            
        except Exception as e:
            print(f"Error calculating kr_shift_last_month: {e}")
            return 0

    def calculate_current_value_for_user(self, user_id):
        """Calculate current OKR value for a specific user"""
        try:
            if self.final_df is None:
                return self.calculate_avg_goals().get(user_id, 0)
            
            user_name = self.user_name_map.get(user_id, '')
            if not user_name:
                return 0
                
            user_df = self.final_df[self.final_df['goal_user_name'] == user_name].copy()
            if user_df.empty:
                return 0
                
            # Calculate current value using average of goal_current_value for unique goal_names
            unique_goals = user_df.groupby('goal_name')['goal_current_value'].first().reset_index()
            unique_goals['goal_current_value'] = pd.to_numeric(unique_goals['goal_current_value'], errors='coerce').fillna(0)
            return unique_goals['goal_current_value'].mean() if len(unique_goals) > 0 else 0
            
        except Exception as e:
            print(f"Error calculating current value for user {user_id}: {e}")
            return 0

    def calculate_final_okr_goal_shift_monthly_for_user(self, user_id):
        """Calculate final_okr_goal_shift_monthly for a specific user"""
        try:
            if self.final_df is None:
                return 0
                
            user_name = self.user_name_map.get(user_id, '')
            if not user_name:
                return 0
                
            user_df = self.final_df[self.final_df['goal_user_name'] == user_name].copy()
            if user_df.empty:
                return 0
            
            reference_month_end = self.get_last_month_end_date()
            unique_combinations = {}
            
            # Process each row to calculate kr_shift_last_month
            for idx, row in user_df.iterrows():
                goal_name = row.get('goal_name', '')
                kr_name = row.get('kr_name', '')
                
                if not goal_name or not kr_name:
                    continue
                
                combo_key = f"{goal_name}|{kr_name}"
                kr_shift = self.calculate_kr_shift_last_month(row, reference_month_end)
                
                if combo_key not in unique_combinations:
                    unique_combinations[combo_key] = []
                unique_combinations[combo_key].append(kr_shift)
            
            # Calculate average for each combination
            final_okr_monthly_shifts = []
            for combo_key, kr_shifts in unique_combinations.items():
                if kr_shifts:
                    avg_kr_shift = sum(kr_shifts) / len(kr_shifts)
                    final_okr_monthly_shifts.append(avg_kr_shift)
            
            # Calculate final average
            if final_okr_monthly_shifts:
                final_okr_goal_shift_monthly = sum(final_okr_monthly_shifts) / len(final_okr_monthly_shifts)
            else:
                final_okr_goal_shift_monthly = 0
            
            return final_okr_goal_shift_monthly
            
        except Exception as e:
            print(f"Error calculating final_okr_goal_shift_monthly for user {user_id}: {e}")
            return 0

    def update_okr_movement(self):
        """Update OKR movement for each user using monthly calculation instead of Google Sheets."""
        if not self.should_calculate_monthly_shift():
            # For months 1, 4, 7, 10 - use current OKR value as movement
            for user in self.users.values():
                current_okr = self.calculate_current_value_for_user(user.user_id)
                user.dich_chuyen_OKR = current_okr
            return

        # For other months, calculate monthly movement
        for user in self.users.values():
            user_id = user.user_id
            
            # Get current OKR value
            current_okr = self.calculate_current_value_for_user(user_id)
            
            # Calculate monthly OKR shift using the integrated monthly calculation
            monthly_shift = self.calculate_final_okr_goal_shift_monthly_for_user(user_id)
            
            # Apply adjustment logic: if shift > current_value, use current_value - last_month_value
            if self.final_df is not None:
                user_name = self.user_name_map.get(user_id, '')
                user_df = self.final_df[self.final_df['goal_user_name'] == user_name].copy()
                
                if not user_df.empty:
                    last_month_end = self.get_last_month_end_date()
                    last_month_value = self.calculate_last_month_value_for_user(user_df, last_month_end)
                    
                    # Adjustment logic
                    if monthly_shift > current_okr:
                        adjusted_shift = current_okr - last_month_value
                        user.dich_chuyen_OKR = round(adjusted_shift, 2)
                    else:
                        user.dich_chuyen_OKR = round(monthly_shift, 2)
                else:
                    user.dich_chuyen_OKR = round(monthly_shift, 2)
            else:
                user.dich_chuyen_OKR = round(monthly_shift, 2)

    def calculate_last_month_value_for_user(self, user_df, last_month_end):
        """Calculate OKR value as of last month end for specific user"""
        try:
            df = user_df.copy()
            df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')

            unique_krs = df['kr_id'].dropna().unique()
            goal_values_dict = {}

            for kr_id in unique_krs:
                kr_data = df[df['kr_id'] == kr_id].copy()
                kr_data['checkin_since_dt'] = pd.to_datetime(kr_data['checkin_since'], errors='coerce')

                actual_checkins_before_month_end = kr_data[
                    (kr_data['checkin_since_dt'] <= last_month_end) &
                    (kr_data['checkin_name'].notna()) &
                    (kr_data['checkin_name'] != '')
                ]

                goal_name = kr_data.iloc[0]['goal_name'] if len(kr_data) > 0 else f"Unknown_{kr_id}"

                if len(actual_checkins_before_month_end) > 0:
                    latest_checkin_before_month_end = actual_checkins_before_month_end.sort_values('checkin_since_dt').iloc[-1]
                    kr_value = pd.to_numeric(latest_checkin_before_month_end['checkin_kr_current_value'], errors='coerce')

                    if pd.isna(kr_value):
                        kr_value = 0

                    if goal_name not in goal_values_dict:
                        goal_values_dict[goal_name] = []
                    goal_values_dict[goal_name].append(kr_value)
                else:
                    kr_value = 0
                    goal_key = f"{goal_name}_no_checkin_{kr_id}"
                    goal_values_dict[goal_key] = [kr_value]

            goal_values = []
            for goal_name, kr_values_list in goal_values_dict.items():
                goal_value = np.mean(kr_values_list)
                goal_values.append(goal_value)

            last_month_value = np.mean(goal_values) if goal_values else 0
            return last_month_value

        except Exception as e:
            print(f"Error calculating last month value: {e}")
            return 0

# Updated function to integrate UserManager with OKRAnalysisSystem
def create_user_manager_with_monthly_calculation(analyzer):
    """Create UserManager integrated with monthly OKR calculation from OKRAnalysisSystem"""
    
    # Get the required dataframes from the analyzer
    account_df = analyzer.filtered_members_df
    
    # Create simplified dataframes for UserManager from final_df
    krs_df = pd.DataFrame()
    checkin_df = pd.DataFrame()
    
    if analyzer.final_df is not None and not analyzer.final_df.empty:
        # Extract KRs data
        krs_data = []
        for _, row in analyzer.final_df.iterrows():
            if pd.notna(row.get('kr_id')):
                # Map user_name back to user_id
                user_name = row.get('goal_user_name', '')
                user_id = None
                for uid, name in analyzer.filtered_members_df.set_index('id')['name'].items():
                    if name == user_name:
                        user_id = uid
                        break
                
                if user_id:
                    krs_data.append({
                        'user_id': user_id,
                        'kr_id': row.get('kr_id'),
                        'current_value': row.get('kr_current_value', 0)
                    })
        
        krs_df = pd.DataFrame(krs_data)
        
        # Extract checkin data with timestamp conversion
        checkin_data = []
        for _, row in analyzer.final_df.iterrows():
            if pd.notna(row.get('checkin_id')):
                user_name = row.get('goal_user_name', '')
                user_id = None
                for uid, name in analyzer.filtered_members_df.set_index('id')['name'].items():
                    if name == user_name:
                        user_id = uid
                        break
                
                if user_id and pd.notna(row.get('checkin_since')):
                    try:
                        # Convert checkin_since to timestamp
                        checkin_datetime = pd.to_datetime(row['checkin_since'])
                        timestamp = checkin_datetime.timestamp()
                        
                        checkin_data.append({
                            'user_id': user_id,
                            'day': timestamp,
                            'checkin_id': row.get('checkin_id')
                        })
                    except:
                        continue
        
        checkin_df = pd.DataFrame(checkin_data)

    # Create UserManager with the integrated final_df
    user_manager = UserManager(account_df, krs_df, checkin_df, analyzer.final_df, analyzer.final_df)
    
    return user_manager

# Add this to your main Streamlit app function
def show_user_score_analysis(analyzer):
    """Show user score analysis using integrated monthly calculation"""
    
    st.subheader("🏆 User Score Analysis (Integrated Monthly Calculation)")
    
    try:
        # Create UserManager with monthly calculation integration
        user_manager = create_user_manager_with_monthly_calculation(analyzer)
        
        # Update checkins
        user_manager.update_checkins()
        
        # Update OKR movement using monthly calculation
        user_manager.update_okr_movement()
        
        # Calculate scores
        user_manager.calculate_scores()
        
        # Get users and create dataframe
        users = user_manager.get_users()
        user_data = []
        
        for user in users:
            user_data.append({
                'Name': user.name,
                'Has OKR': 'Yes' if user.co_OKR == 1 else 'No',
                'Check-in': 'Yes' if user.checkin == 1 else 'No',
                'OKR Movement': user.dich_chuyen_OKR,
                'Score': user.score
            })
        
        scores_df = pd.DataFrame(user_data)
        
        if not scores_df.empty:
            # Display summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                avg_score = scores_df['Score'].mean()
                st.metric("Average Score", f"{avg_score:.2f}")
            
            with col2:
                high_performers = len(scores_df[scores_df['Score'] >= 3.0])
                st.metric("High Performers (≥3.0)", high_performers)
            
            with col3:
                low_performers = len(scores_df[scores_df['Score'] < 2.0])
                st.metric("Need Support (<2.0)", low_performers)
            
            with col4:
                has_okr_count = len(scores_df[scores_df['Has OKR'] == 'Yes'])
                st.metric("Has OKR", f"{has_okr_count}/{len(scores_df)}")
            
            # Score distribution chart
            fig_scores = px.histogram(
                scores_df, 
                x='Score',
                nbins=20,
                title="Distribution of User Scores (with Monthly OKR Calculation)",
                labels={'Score': 'User Score', 'count': 'Number of Users'}
            )
            st.plotly_chart(fig_scores, use_container_width=True)
            
            # Top performers table
            st.subheader("🏆 Top Performers")
            top_performers = scores_df.nlargest(10, 'Score')
            st.dataframe(top_performers, use_container_width=True, hide_index=True)
            
            # Users needing support
            low_performers_df = scores_df[scores_df['Score'] < 2.0]
            if not low_performers_df.empty:
                st.subheader("⚠️ Users Needing Support")
                st.dataframe(low_performers_df, use_container_width=True, hide_index=True)
            
            # Export option
            if st.button("📊 Export User Scores"):
                csv = scores_df.to_csv(index=False)
                st.download_button(
                    label="Download User Scores CSV",
                    data=csv,
                    file_name=f"user_scores_monthly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            
            # Excel export option
            if st.button("📋 Export to Excel Format"):
                wb = export_to_excel(users)
                
                # Save to BytesIO
                excel_buffer = BytesIO()
                wb.save(excel_buffer)
                excel_buffer.seek(0)
                
                st.download_button(
                    label="Download Excel Report",
                    data=excel_buffer.getvalue(),
                    file_name=f"okr_report_monthly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            return scores_df
            
        else:
            st.warning("No user score data available")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error in user score analysis: {e}")
        return pd.DataFrame()

# Add the required helper function
def get_current_quarter_start():
    """Get current quarter start date"""
    today = datetime.now()
    quarter = (today.month - 1) // 3 + 1
    quarter_start_month = (quarter - 1) * 3 + 1
    return datetime(today.year, quarter_start_month, 1)

class APIClient:
    """Client for handling API requests"""
    
    def __init__(self, goal_token: str, account_token: str):
        self.goal_token = goal_token
        self.account_token = account_token
        self.timeout = 30

    def make_request(self, url: str, data: Dict, description: str = "") -> requests.Response:
        """Make HTTP request with error handling"""
        try:
            response = requests.post(url, data=data, timeout=self.timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            st.error(f"Error {description}: {e}")
            raise

    def get_filtered_members(self) -> pd.DataFrame:
        """Get filtered members from account API"""
        url = "https://account.base.vn/extapi/v1/group/get"
        data = {"access_token": self.account_token, "path": "aplus"}
        
        response = self.make_request(url, data, "fetching account members")
        response_data = response.json()
        
        group = response_data.get('group', {})
        members = group.get('members', [])
        
        df = pd.DataFrame([
            {
                'id': str(m.get('id', '')),
                'name': m.get('name', ''),
                'username': m.get('username', ''),
                'job': m.get('title', ''),
                'email': m.get('email', '')
            }
            for m in members
        ])
        
        # Apply filters
        filtered_df = df[~df['job'].str.lower().str.contains(
            'kcs|agile|khu vực|sa ti co|trainer|specialist|no|chuyên gia|xnk|vat|trưởng phòng thị trường', 
            na=False
        )]
        filtered_df = filtered_df[filtered_df['username'] != 'ThuAn']
        
        return filtered_df

    def get_cycle_list(self) -> List[Dict]:
        """Get list of quarterly cycles sorted by most recent first"""
        url = "https://goal.base.vn/extapi/v1/cycle/list"
        data = {'access_token': self.goal_token}

        response = self.make_request(url, data, "fetching cycle list")
        data = response.json()

        quarterly_cycles = []
        for cycle in data.get('cycles', []):
            if cycle.get('metatype') == 'quarterly':
                try:
                    start_time = datetime.fromtimestamp(float(cycle['start_time']), tz=timezone.utc)
                    quarterly_cycles.append({
                        'name': cycle['name'],
                        'path': cycle['path'],
                        'start_time': start_time,
                        'formatted_start_time': start_time.strftime('%d/%m/%Y')
                    })
                except (ValueError, TypeError) as e:
                    st.warning(f"Error parsing cycle {cycle.get('name', 'Unknown')}: {e}")
                    continue

        return sorted(quarterly_cycles, key=lambda x: x['start_time'], reverse=True)

    def get_account_users(self) -> pd.DataFrame:
        """Get users from Account API"""
        url = "https://account.base.vn/extapi/v1/users"
        data = {"access_token": self.account_token}

        response = self.make_request(url, data, "fetching account users")
        json_response = response.json()
        
        if isinstance(json_response, list) and len(json_response) > 0:
            json_response = json_response[0]

        account_users = json_response.get('users', [])
        return pd.DataFrame([{
            'id': str(user['id']),
            'name': user['name'],
            'username': user['username']
        } for user in account_users])

    def get_goals_data(self, cycle_path: str) -> pd.DataFrame:
        """Get goals data from API"""
        url = "https://goal.base.vn/extapi/v1/cycle/get.full"
        data = {'access_token': self.goal_token, 'path': cycle_path}

        response = self.make_request(url, data, "fetching goals data")
        data = response.json()

        goals_data = []
        for goal in data.get('goals', []):
            goal_data = {
                'goal_id': goal.get('id'),
                'goal_name': goal.get('name', 'Unknown Goal'),
                'goal_content': goal.get('content', ''),
                'goal_since': DateUtils.convert_timestamp_to_datetime(goal.get('since')),
                'goal_current_value': goal.get('current_value', 0),
                'goal_user_id': str(goal.get('user_id', '')),
            }
            goals_data.append(goal_data)

        return pd.DataFrame(goals_data)

    def get_krs_data(self, cycle_path: str) -> pd.DataFrame:
        """Get KRs data from API with pagination"""
        url = "https://goal.base.vn/extapi/v1/cycle/krs"
        all_krs = []
        page = 1
        max_pages = 50

        progress_bar = st.progress(0)
        status_text = st.empty()

        while page <= max_pages:
            status_text.text(f"Loading KRs... Page {page}")
            data = {"access_token": self.goal_token, "path": cycle_path, "page": page}

            response = self.make_request(url, data, f"loading KRs at page {page}")
            response_data = response.json()

            if isinstance(response_data, list) and len(response_data) > 0:
                response_data = response_data[0]

            krs_list = response_data.get("krs", [])
            if not krs_list:
                break

            for kr in krs_list:
                kr_data = {
                    'kr_id': str(kr.get('id', '')),
                    'kr_name': kr.get('name', 'Unknown KR'),
                    'kr_content': kr.get('content', ''),
                    'kr_since': DateUtils.convert_timestamp_to_datetime(kr.get('since')),
                    'kr_current_value': kr.get('current_value', 0),
                    'kr_user_id': str(kr.get('user_id', '')),
                    'goal_id': kr.get('goal_id'),
                }
                all_krs.append(kr_data)

            progress_bar.progress(min(page / 10, 1.0))
            page += 1

        progress_bar.empty()
        status_text.empty()
        return pd.DataFrame(all_krs)

    def get_all_checkins(self, cycle_path: str) -> List[Dict]:
        """Get all checkins with pagination"""
        url = "https://goal.base.vn/extapi/v1/cycle/checkins"
        all_checkins = []
        page = 1
        max_pages = 100

        progress_bar = st.progress(0)
        status_text = st.empty()

        while page <= max_pages:
            status_text.text(f"Loading checkins... Page {page}")
            data = {"access_token": self.goal_token, "path": cycle_path, "page": page}

            response = self.make_request(url, data, f"loading checkins at page {page}")
            response_data = response.json()

            if isinstance(response_data, list) and len(response_data) > 0:
                response_data = response_data[0]

            checkins = response_data.get('checkins', [])
            if not checkins:
                break

            all_checkins.extend(checkins)
            progress_bar.progress(min(page / 20, 1.0))

            if len(checkins) < 20:
                break

            page += 1

        progress_bar.empty()
        status_text.empty()
        return all_checkins

class DataProcessor:
    """Handles data processing and transformations"""
    
    @staticmethod
    def extract_checkin_data(all_checkins: List[Dict]) -> pd.DataFrame:
        """Extract checkin data into DataFrame"""
        checkin_list = []

        for checkin in all_checkins:
            try:
                checkin_id = checkin.get('id', '')
                checkin_name = checkin.get('name', '')
                user_id = str(checkin.get('user_id', ''))
                since_timestamp = checkin.get('since', '')

                # Convert timestamp
                since_date = DateUtils.convert_timestamp_to_datetime(since_timestamp) or ''

                # Extract form value
                form_value = ''
                form_data = checkin.get('form', [])
                if form_data and len(form_data) > 0:
                    form_value = form_data[0].get('value', '')

                # Extract target info
                target_name = ''
                kr_id = ''
                current_value = checkin.get('current_value', 0)

                obj_export = checkin.get('obj_export', {})
                if obj_export:
                    target_name = obj_export.get('name', '')
                    kr_id = str(obj_export.get('id', ''))

                checkin_list.append({
                    'checkin_id': checkin_id,
                    'checkin_name': checkin_name,
                    'checkin_since': since_date,
                    'cong_viec_tiep_theo': form_value,
                    'checkin_target_name': target_name,
                    'checkin_kr_current_value': current_value,
                    'kr_id': kr_id,
                    'checkin_user_id': user_id
                })
            except Exception as e:
                st.warning(f"Error processing checkin {checkin.get('id', 'Unknown')}: {e}")
                continue

        return pd.DataFrame(checkin_list)

    @staticmethod
    def clean_final_data(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare final dataset"""
        try:
            # Fill NaN values
            df['kr_current_value'] = pd.to_numeric(df['kr_current_value'], errors='coerce').fillna(0.00)
            df['checkin_kr_current_value'] = pd.to_numeric(df['checkin_kr_current_value'], errors='coerce').fillna(0.00)

            # Fill dates
            df['kr_since'] = df['kr_since'].fillna(df['goal_since'])
            df['checkin_since'] = df['checkin_since'].fillna(df['kr_since'])

            # Drop unused columns
            columns_to_drop = ['goal_user_id', 'kr_user_id']
            existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
            if existing_columns_to_drop:
                df = df.drop(columns=existing_columns_to_drop)

            return df
        except Exception as e:
            st.error(f"Error cleaning data: {e}")
            return df

class OKRCalculator:
    """Handles OKR calculations and analysis"""
    
    @staticmethod
    def calculate_current_value(df: pd.DataFrame) -> float:
        """Calculate current OKR value using average of goal_current_value for unique goal_names"""
        try:
            unique_goals = df.groupby('goal_name')['goal_current_value'].first().reset_index()
            unique_goals['goal_current_value'] = pd.to_numeric(unique_goals['goal_current_value'], errors='coerce').fillna(0)
            return unique_goals['goal_current_value'].mean() if len(unique_goals) > 0 else 0
        except Exception as e:
            st.error(f"Error calculating current value: {e}")
            return 0

    @staticmethod
    def calculate_last_friday_value(last_friday: datetime, df: pd.DataFrame) -> Tuple[float, List[Dict]]:
        """Calculate OKR value as of last Friday"""
        try:
            df = df.copy()
            df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')

            unique_krs = df['kr_id'].dropna().unique()
            goal_values_dict = {}
            kr_details = []

            for kr_id in unique_krs:
                kr_data = df[df['kr_id'] == kr_id].copy()
                kr_data['checkin_since_dt'] = pd.to_datetime(kr_data['checkin_since'], errors='coerce')

                actual_checkins_before_friday = kr_data[
                    (kr_data['checkin_since_dt'] <= last_friday) &
                    (kr_data['checkin_name'].notna()) &
                    (kr_data['checkin_name'] != '')
                ]

                goal_name = kr_data.iloc[0]['goal_name'] if len(kr_data) > 0 else f"Unknown_{kr_id}"

                if len(actual_checkins_before_friday) > 0:
                    latest_checkin_before_friday = actual_checkins_before_friday.sort_values('checkin_since_dt').iloc[-1]
                    kr_value = pd.to_numeric(latest_checkin_before_friday['checkin_kr_current_value'], errors='coerce')

                    if pd.isna(kr_value):
                        kr_value = 0

                    if goal_name not in goal_values_dict:
                        goal_values_dict[goal_name] = []
                    goal_values_dict[goal_name].append(kr_value)

                    kr_details.append({
                        'kr_id': kr_id,
                        'goal_name': goal_name,
                        'kr_value': kr_value,
                        'checkin_date': latest_checkin_before_friday['checkin_since_dt'],
                        'source': 'checkin_before_friday'
                    })
                else:
                    kr_value = 0
                    goal_key = f"{goal_name}_no_checkin_{kr_id}"
                    goal_values_dict[goal_key] = [kr_value]

                    kr_details.append({
                        'kr_id': kr_id,
                        'goal_name': goal_name,
                        'kr_value': kr_value,
                        'checkin_date': None,
                        'source': 'no_checkin_before_friday'
                    })

            goal_values = []
            for goal_name, kr_values_list in goal_values_dict.items():
                goal_value = np.mean(kr_values_list)
                goal_values.append(goal_value)

            last_friday_value = np.mean(goal_values) if goal_values else 0
            return last_friday_value, kr_details

        except Exception as e:
            st.error(f"Error calculating last Friday value: {e}")
            return 0, []

    @staticmethod
    def calculate_kr_shift_last_friday(row: pd.Series, last_friday: datetime, final_df: pd.DataFrame) -> float:
        """Calculate kr_shift_last_friday = kr_current_value - last_friday_checkin_value"""
        try:
            kr_current_value = pd.to_numeric(row.get('kr_current_value', 0), errors='coerce')
            if pd.isna(kr_current_value):
                kr_current_value = 0
            
            kr_id = row.get('kr_id', '')
            if not kr_id:
                return kr_current_value
            
            quarter_start = DateUtils.get_quarter_start_date()
            reference_friday = DateUtils.get_last_friday_date()
            
            kr_checkins = final_df[
                (final_df['kr_id'] == kr_id) & 
                (final_df['checkin_id'].notna()) &
                (final_df['checkin_name'].notna()) &
                (final_df['checkin_name'] != '')
            ].copy()
            
            if not kr_checkins.empty:
                kr_checkins['checkin_since_dt'] = pd.to_datetime(kr_checkins['checkin_since'], errors='coerce')
                kr_checkins = kr_checkins[
                    (kr_checkins['checkin_since_dt'] >= quarter_start) &
                    (kr_checkins['checkin_since_dt'] <= reference_friday)
                ]
                
                if not kr_checkins.empty:
                    latest_checkin = kr_checkins.loc[kr_checkins['checkin_since_dt'].idxmax()]
                    last_friday_checkin_value = pd.to_numeric(latest_checkin.get('checkin_kr_current_value', 0), errors='coerce')
                    if pd.isna(last_friday_checkin_value):
                        last_friday_checkin_value = 0
                else:
                    last_friday_checkin_value = 0
            else:
                last_friday_checkin_value = 0
            
            kr_shift = kr_current_value - last_friday_checkin_value
            return kr_shift
            
        except Exception as e:
            st.warning(f"Error calculating kr_shift_last_friday: {e}")
            return 0

class EmailReportGenerator:
    """Generate and send email reports for OKR analysis"""
    
    def __init__(self, smtp_server="smtp.gmail.com", smtp_port=587):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def create_visual_html_chart(self, data, chart_type, title):
        """Create HTML-based visual charts"""
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
        
    def _generate_okr_table_html_monthly(self, data):
        """Generate HTML table for monthly OKR data"""
        if not data:
            return "<div style='text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; color: #7f8c8d;'><p>📭 Không có dữ liệu</p></div>"
        
        html = """
        <table>
            <thead>
                <tr>
                    <th>👤 Nhân viên</th>
                    <th>📊 Dịch chuyển (tháng)</th>
                    <th>🎯 Giá trị hiện tại</th>
                    <th>📅 Giá trị cuối tháng trước</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for i, item in enumerate(data):
            shift_class = "positive" if item['okr_shift_monthly'] > 0 else "negative" if item['okr_shift_monthly'] < 0 else "neutral"
            shift_icon = "📈" if item['okr_shift_monthly'] > 0 else "📉" if item['okr_shift_monthly'] < 0 else "➡️"
            row_class = "even" if i % 2 == 0 else "odd"
            
            html += f"""
            <tr class='{row_class}'>
                <td><strong>{item['user_name']}</strong></td>
                <td class="{shift_class}">{shift_icon} <strong>{item['okr_shift_monthly']:.2f}</strong></td>
                <td><span style='color: #3498db; font-weight: 600;'>{item['current_value']:.2f}</span></td>
                <td><span style='color: #7f8c8d;'>{item['last_month_value']:.2f}</span></td>
            </tr>
            """
        
        html += "</tbody></table>"
        return html

    def _generate_top_overall_table_html(self, overall_checkins_data):
        """Generate HTML table for top overall checkin users"""
        if not overall_checkins_data:
            return "<div style='text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; color: #7f8c8d;'><p>📭 Không có dữ liệu</p></div>"
        
        today = datetime.now()
        quarter_start = datetime(today.year, ((today.month - 1) // 3) * 3 + 1, 1)
        weeks_in_quarter = (today - quarter_start).days / 7
        weeks_in_quarter = max(weeks_in_quarter, 1)
        
        days_since_monday = today.weekday()
        monday_this_week = today - timedelta(days=days_since_monday)
        monday_last_week = monday_this_week - timedelta(days=7)
        sunday_last_week = monday_last_week + timedelta(days=6)
        
        html = f"""
        <div class="alert alert-info">
            <strong>📅 Tuần trước:</strong> {monday_last_week.strftime('%d/%m/%Y')} - {sunday_last_week.strftime('%d/%m/%Y')}<br>
            <strong>📊 Tần suất checkin:</strong> Tổng checkin ÷ {weeks_in_quarter:.1f} tuần (từ đầu quý đến nay)
        </div>
        <table>
            <thead>
                <tr>
                    <th>🏅 Hạng</th>
                    <th>👤 Nhân viên</th>
                    <th>📊 Tổng checkin</th>
                    <th>⚡ Tần suất/tuần (quý)</th>
                    <th>📅 Checkin tuần trước</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for i, item in enumerate(overall_checkins_data):
            rank_icon = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i+1}"
            frequency = item.get('checkin_frequency_per_week', 0)
            last_week = item.get('last_week_checkins', 0)
            total = item.get('total_checkins', 0)
            
            row_style = ""
            if i < 3:
                row_style = "style='background: linear-gradient(135deg, #fff9e6, #fffbf0); font-weight: 600;'"
            elif i % 2 == 0:
                row_style = "style='background: #f8f9fa;'"
            
            frequency_style = "style='color: #27AE60; font-weight: bold;'" if frequency >= 2 else ""
            last_week_style = "style='color: #3498db; font-weight: bold;'" if last_week > 0 else "style='color: #7f8c8d;'"
            
            html += f"""
            <tr {row_style}>
                <td style='text-align: center; font-size: 16px; font-weight: bold;'>{rank_icon}</td>
                <td><strong>{item.get('user_name', 'Unknown')}</strong></td>
                <td style='text-align: center; font-weight: 600; color: #2c3e50;'>{total}</td>
                <td style='text-align: center;' {frequency_style}>{frequency:.2f}</td>
                <td style='text-align: center;' {last_week_style}>{last_week}</td>
            </tr>
            """
        
        if overall_checkins_data:
            total_checkins_sum = sum(item.get('total_checkins', 0) for item in overall_checkins_data)
            avg_frequency = sum(item.get('checkin_frequency_per_week', 0) for item in overall_checkins_data) / len(overall_checkins_data)
            active_last_week = len([item for item in overall_checkins_data if item.get('last_week_checkins', 0) > 0])
            
            html += f"""
            <tr style='background: linear-gradient(135deg, #e8f4f8, #f0f8ff); border-top: 2px solid #3498db; font-weight: bold;'>
                <td colspan="2" style='text-align: center; color: #2c3e50;'>📊 TỔNG KẾT TOP {len(overall_checkins_data)}</td>
                <td style='text-align: center; color: #3498db;'>{total_checkins_sum}</td>
                <td style='text-align: center; color: #27AE60;'>{avg_frequency:.2f}</td>
                <td style='text-align: center; color: #e74c3c;'>{active_last_week} người</td>
            </tr>
            """
        
        html += "</tbody></table>"
        return html

    def create_email_content(self, analyzer, selected_cycle, members_without_goals, members_without_checkins, 
                               members_with_goals_no_checkins, okr_shifts, okr_shifts_monthly=None):
            """Create HTML email content with fallback charts including monthly data when applicable"""
            
            current_date = datetime.now().strftime("%d/%m/%Y")
            total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
            
            # Calculate statistics
            members_with_goals = total_members - len(members_without_goals)
            members_with_checkins = total_members - len(members_without_checkins)
            
            progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0]) if okr_shifts else 0
            stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0]) if okr_shifts else 0
            issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0]) if okr_shifts else 0
            
            # Monthly statistics (if available)
            monthly_stats = {}
            if okr_shifts_monthly:
                monthly_stats = {
                    'progress_users_monthly': len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] > 0]),
                    'stable_users_monthly': len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] == 0]),
                    'issue_users_monthly': len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] < 0])
                }
            
            # Get checkin behavior analysis data
            period_checkins, overall_checkins = analyzer.analyze_checkin_behavior()
            
            # Create visual charts - weekly
            goal_chart = self.create_visual_html_chart(
                {'Có OKR': members_with_goals, 'Chưa có OKR': len(members_without_goals)},
                'pie', 'Phân bố trạng thái OKR'
            )
            
            okr_shifts_data = {u['user_name']: u['okr_shift'] for u in okr_shifts[:15]} if okr_shifts else {}
            okr_shifts_chart = self.create_visual_html_chart(
                okr_shifts_data, 'bar', 'Dịch chuyển OKR tuần (Top 15)'
            )
            
            # Create monthly chart if available
            monthly_chart_html = ""
            if okr_shifts_monthly:
                okr_shifts_monthly_data = {u['user_name']: u['okr_shift_monthly'] for u in okr_shifts_monthly[:15]}
                monthly_chart_html = self.create_visual_html_chart(
                    okr_shifts_monthly_data, 'bar', 'Dịch chuyển OKR tháng (Top 15)'
                )
            
            # Generate tables
            goals_table = self._generate_table_html(members_without_goals, 
                                                   ["Tên", "Username", "Chức vụ"], 
                                                   ["name", "username", "job"])
            
            checkins_table = self._generate_table_html(members_without_checkins,
                                                     ["Tên", "Username", "Chức vụ", "Có OKR"],
                                                     ["name", "username", "job", "has_goal"])
            
            goals_no_checkins_table = self._generate_table_html(members_with_goals_no_checkins,
                                                              ["Tên", "Username", "Chức vụ"],
                                                              ["name", "username", "job"])
            
            # Top performers tables
            top_performers = [u for u in okr_shifts if u['okr_shift'] > 0][:10] if okr_shifts else []
            top_performers_table = self._generate_okr_table_html(top_performers)
            
            top_performers_monthly = [u for u in okr_shifts_monthly if u['okr_shift_monthly'] > 0][:10] if okr_shifts_monthly else []
            top_performers_monthly_table = self._generate_okr_table_html_monthly(top_performers_monthly) if top_performers_monthly else ""
            
            # Issue users tables
            issue_performers = [u for u in okr_shifts if u['okr_shift'] < 0][:10] if okr_shifts else []
            issue_performers_table = self._generate_okr_table_html(issue_performers)
            
            issue_performers_monthly = [u for u in okr_shifts_monthly if u['okr_shift_monthly'] < 0][:10] if okr_shifts_monthly else []
            issue_performers_monthly_table = self._generate_okr_table_html_monthly(issue_performers_monthly) if issue_performers_monthly else ""
            
            # Top overall table
            top_overall_table = self._generate_top_overall_table_html(overall_checkins[:20] if overall_checkins else [])
            
            # Start building HTML content
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
                    .positive {{ color: #27AE60; font-weight: bold; }}
                    .negative {{ color: #E74C3C; font-weight: bold; }}
                    .neutral {{ color: #F39C12; font-weight: bold; }}
                    .footer {{ text-align: center; margin-top: 40px; padding: 25px; background: linear-gradient(135deg, #2c3e50, #34495e); color: white; border-radius: 15px; }}
                    .alert {{ padding: 18px; margin: 20px 0; border-radius: 10px; border-left: 4px solid; }}
                    .alert-warning {{ background: linear-gradient(135deg, #fff3cd, #fef8e6); border-left-color: #f39c12; color: #856404; }}
                    .alert-info {{ background: linear-gradient(135deg, #d1ecf1, #e8f5f7); border-left-color: #3498db; color: #0c5460; }}
                    .monthly-indicator {{ background: linear-gradient(135deg, #e8f5e8, #f0fff0); border: 2px solid #27AE60; border-radius: 10px; padding: 15px; margin: 20px 0; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>📊 BÁO CÁO TIẾN ĐỘ OKR & CHECKIN</h1>
                    <h2>{selected_cycle['name']}</h2>
                    <p>Ngày báo cáo: {current_date}</p>
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
                            <div class="metric-label">Tiến bộ (tuần)</div>
                        </div>
            """
            
            # Add monthly metric if available
            if monthly_stats:
                html_content += f"""
                        <div class="metric">
                            <div class="metric-value">{monthly_stats['progress_users_monthly']}</div>
                            <div class="metric-label">Tiến bộ (tháng)</div>
                        </div>
                """
            
            html_content += """
                    </div>
                </div>
            """
            
            # Add monthly indicator if applicable
            if okr_shifts_monthly:
                current_month = datetime.now().month
                month_name = {2: "Tháng 2", 3: "Tháng 3", 5: "Tháng 5", 6: "Tháng 6", 
                             8: "Tháng 8", 9: "Tháng 9", 11: "Tháng 11", 12: "Tháng 12"}.get(current_month, f"Tháng {current_month}")
                
                html_content += f"""
                <div class="monthly-indicator">
                    <strong>🗓️ {month_name}:</strong> Báo cáo này bao gồm phân tích dịch chuyển OKR theo tháng (so với cuối tháng trước)
                </div>
                """
            
            # Continue with existing sections...
            html_content += f"""
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
                    <h2>📊 DỊCH CHUYỂN OKR (TUẦN)</h2>
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
            
            # Add monthly OKR section if available
            if monthly_chart_html:
                html_content += f"""
                <div class="section">
                    <h2>🗓️ DỊCH CHUYỂN OKR (THÁNG)</h2>
                    <div class="chart-container">
                        {monthly_chart_html}
                    </div>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-value positive">{monthly_stats['progress_users_monthly']}</div>
                            <div class="metric-label">Tiến bộ</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value neutral">{monthly_stats['stable_users_monthly']}</div>
                            <div class="metric-label">Ổn định</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value negative">{monthly_stats['issue_users_monthly']}</div>
                            <div class="metric-label">Cần quan tâm</div>
                        </div>
                    </div>
                </div>
                """
            
            # Continue with rest of email content (checkin analysis, detailed tables, etc.)
            html_content += f"""
                <div class="section">
                    <h2>🏆 TOP NHÂN VIÊN HOẠT ĐỘNG CHECKIN NHIỀU NHẤT</h2>
                    <div class="alert alert-info">
                        <strong>Thống kê:</strong> Xếp hạng dựa trên tổng số checkin và tần suất checkin từ đầu quý
                    </div>
                    {top_overall_table}
                </div>
            """
            
            # Add detailed tables for goals, checkins, top performers, etc. (keeping existing logic)
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
                    <h2>🏆 TOP NHÂN VIÊN TIẾN BỘ NHẤT (TUẦN)</h2>
                    {top_performers_table}
                </div>
                """
            
            if top_performers_monthly_table:
                html_content += f"""
                <div class="section">
                    <h2>🗓️ TOP NHÂN VIÊN TIẾN BỘ NHẤT (THÁNG)</h2>
                    {top_performers_monthly_table}
                </div>
                """
            
            if issue_performers:
                html_content += f"""
                <div class="section">
                    <h2>⚠️ NHÂN VIÊN CẦN HỖ TRỢ (TUẦN)</h2>
                    <div class="alert alert-warning">
                        <strong>Cần quan tâm:</strong> OKR của những nhân viên này đang giảm hoặc không tiến triển.
                    </div>
                    {issue_performers_table}
                </div>
                """
            
            if issue_performers_monthly_table:
                html_content += f"""
                <div class="section">
                    <h2>🗓️ NHÂN VIÊN CẦN HỖ TRỢ (THÁNG)</h2>
                    <div class="alert alert-warning">
                        <strong>Cần quan tâm:</strong> OKR tháng của những nhân viên này đang giảm hoặc không tiến triển.
                    </div>
                    {issue_performers_monthly_table}
                </div>
                """
            
            html_content += """
                <div class="footer">
                    <p><strong>🏢 A Plus Mineral Material Corporation</strong></p>
                    <p>📊 Báo cáo được tạo tự động bởi hệ thống OKR Analysis</p>
                    <p><em>📧 Đây là email tự động, vui lòng không trả lời email này.</em></p>
                </div>
            </body>
            </html>
            """
            
            return html_content

    def send_email_report(self, email_from, password, email_to, subject, html_content, 
                         company_name="A Plus Mineral Material Corporation"):
        """Send email report with improved compatibility"""
        try:
            message = MIMEMultipart('related')
            message['From'] = f"OKR System {company_name} <{email_from}>"
            message['To'] = email_to
            message['Subject'] = subject
            
            msg_alternative = MIMEMultipart('alternative')
            message.attach(msg_alternative)
            
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg_alternative.attach(html_part)
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(email_from, password)
            
            server.send_message(message)
            server.quit()
            
            return True, "Email sent successfully!"
            
        except smtplib.SMTPAuthenticationError:
            return False, "Lỗi xác thực: Vui lòng kiểm tra lại email và mật khẩu"
        except Exception as e:
            return False, f"Lỗi gửi email: {str(e)}"

class OKRAnalysisSystem:
    """Main OKR Analysis System combining all components"""

    def __init__(self, goal_access_token: str, account_access_token: str):
        self.api_client = APIClient(goal_access_token, account_access_token)
        self.data_processor = DataProcessor()
        self.okr_calculator = OKRCalculator()
        self.checkin_path = None
        self.final_df = None
        self.filtered_members_df = None

    def get_filtered_members(self) -> pd.DataFrame:
        """Get filtered members from account API"""
        self.filtered_members_df = self.api_client.get_filtered_members()
        return self.filtered_members_df

    def get_cycle_list(self) -> List[Dict]:
        """Get list of quarterly cycles"""
        return self.api_client.get_cycle_list()

    def load_and_process_data(self, progress_callback=None):
        """Main function to load and process all data"""
        try:
            if progress_callback:
                progress_callback("Loading filtered members...", 0.05)
            
            filtered_members = self.get_filtered_members()
            if filtered_members.empty:
                st.error("Failed to load filtered members data")
                return None

            if progress_callback:
                progress_callback("Loading users...", 0.1)
            
            users_df = self.api_client.get_account_users()
            if users_df.empty:
                st.error("Failed to load users data")
                return None
            
            user_id_to_name = dict(zip(users_df['id'], users_df['name']))

            if progress_callback:
                progress_callback("Loading goals...", 0.2)
            
            goals_df = self.api_client.get_goals_data(self.checkin_path)
            if goals_df.empty:
                st.error("Failed to load goals data")
                return None

            if progress_callback:
                progress_callback("Loading KRs...", 0.4)
            
            krs_df = self.api_client.get_krs_data(self.checkin_path)

            if progress_callback:
                progress_callback("Loading checkins...", 0.6)
            
            all_checkins = self.api_client.get_all_checkins(self.checkin_path)
            checkin_df = self.data_processor.extract_checkin_data(all_checkins)

            if progress_callback:
                progress_callback("Processing data...", 0.8)

            # Join all data
            joined_df = goals_df.merge(krs_df, on='goal_id', how='left')
            joined_df['goal_user_name'] = joined_df['goal_user_id'].map(user_id_to_name)
            self.final_df = joined_df.merge(checkin_df, on='kr_id', how='left')

            # Clean data
            self.final_df = self.data_processor.clean_final_data(self.final_df)

            if progress_callback:
                progress_callback("Data processing completed!", 1.0)

            return self.final_df

        except Exception as e:
            st.error(f"Error in data processing: {e}")
            return None

    def analyze_missing_goals_and_checkins(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Analyze members without goals and without checkins"""
        try:
            if self.filtered_members_df is None or self.final_df is None:
                return [], [], []

            users_with_goals = set(self.final_df['goal_user_name'].dropna().unique())
            
            users_with_checkins = set()
            if 'checkin_user_id' in self.final_df.columns:
                user_id_to_name = dict(zip(self.filtered_members_df['id'], self.filtered_members_df['name']))
                checkin_user_ids = self.final_df['checkin_user_id'].dropna().unique()
                users_with_checkins = {user_id_to_name.get(uid, uid) for uid in checkin_user_ids if uid in user_id_to_name}
            
            all_members = set(self.filtered_members_df['name'].unique())
            
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

    def calculate_final_okr_goal_shift(self, user_df: pd.DataFrame) -> float:
        """Calculate final_okr_goal_shift using reference to previous Friday"""
        try:
            reference_friday = DateUtils.get_last_friday_date()
            
            unique_combinations = {}
            
            for idx, row in user_df.iterrows():
                goal_name = row.get('goal_name', '')
                kr_name = row.get('kr_name', '')
                
                if not goal_name or not kr_name:
                    continue
                
                combo_key = f"{goal_name}|{kr_name}"
                
                kr_shift = self.okr_calculator.calculate_kr_shift_last_friday(row, reference_friday, self.final_df)
                
                if combo_key not in unique_combinations:
                    unique_combinations[combo_key] = []
                unique_combinations[combo_key].append(kr_shift)
            
            final_okr_friday_shifts = []
            
            for combo_key, kr_shifts in unique_combinations.items():
                if kr_shifts:
                    avg_kr_shift = sum(kr_shifts) / len(kr_shifts)
                    final_okr_friday_shifts.append(avg_kr_shift)
            
            if final_okr_friday_shifts:
                final_okr_goal_shift = sum(final_okr_friday_shifts) / len(final_okr_friday_shifts)
            else:
                final_okr_goal_shift = 0
            
            return final_okr_goal_shift
            
        except Exception as e:
            st.error(f"Error calculating final_okr_goal_shift: {e}")
            return 0

    def calculate_okr_shifts_by_user(self) -> List[Dict]:
        """Calculate OKR shifts for each user"""
        try:
            users = self.final_df['goal_user_name'].dropna().unique()
            user_okr_shifts = []
    
            reference_friday = DateUtils.get_last_friday_date()
    
            for user in users:
                user_df = self.final_df[self.final_df['goal_user_name'] == user].copy()
                
                final_okr_goal_shift = self.calculate_final_okr_goal_shift(user_df)
                
                current_value = self.okr_calculator.calculate_current_value(user_df)
                last_friday_value, kr_details = self.okr_calculator.calculate_last_friday_value(reference_friday, user_df)
                
                legacy_okr_shift = current_value - last_friday_value
    
                adjusted_okr_shift = final_okr_goal_shift
                adjustment_applied = False
                
                if final_okr_goal_shift > current_value:
                    adjusted_okr_shift = current_value - last_friday_value
                    adjustment_applied = True

                if current_value < last_friday_value or last_friday_value != current_value - final_okr_goal_shift:
                    last_friday_value = current_value - final_okr_goal_shift
                
                user_okr_shifts.append({
                    'user_name': user,
                    'okr_shift': adjusted_okr_shift,
                    'original_shift': final_okr_goal_shift,
                    'current_value': current_value,
                    'last_friday_value': last_friday_value,
                    'legacy_okr_shift': legacy_okr_shift,
                    'adjustment_applied': adjustment_applied,
                    'kr_details_count': len(kr_details),
                    'reference_friday': reference_friday.strftime('%d/%m/%Y')
                })
    
            return sorted(user_okr_shifts, key=lambda x: x['okr_shift'], reverse=True)
    
        except Exception as e:
            st.error(f"Error calculating OKR shifts: {e}")
            return []

    def analyze_checkin_behavior(self) -> Tuple[List[Dict], List[Dict]]:
        """Analyze checkin behavior"""
        try:
            last_friday = DateUtils.get_last_friday_date()
            quarter_start = DateUtils.get_quarter_start_date()

            df = self.final_df.copy()
            df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')

            mask_period = (df['checkin_since_dt'] >= quarter_start) & (df['checkin_since_dt'] <= last_friday)
            period_df = df[mask_period].copy()

            all_time_df = df[df['checkin_id'].notna()].copy()

            all_users = df['goal_user_name'].dropna().unique()

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
        
        today = datetime.now()
        days_since_monday = today.weekday()
        monday_this_week = today - timedelta(days=days_since_monday)
        monday_last_week = monday_this_week - timedelta(days=7)
        sunday_last_week = monday_last_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
        quarter_start = DateUtils.get_quarter_start_date()
        weeks_in_quarter = (today - quarter_start).days / 7
        weeks_in_quarter = max(weeks_in_quarter, 1)
    
        for user in all_users:
            try:
                user_total_checkins = all_time_df[all_time_df['goal_user_name'] == user]['checkin_id'].nunique()
                user_total_krs = full_df[full_df['goal_user_name'] == user]['kr_id'].nunique()
                checkin_rate = (user_total_checkins / user_total_krs * 100) if user_total_krs > 0 else 0
    
                user_checkins_dates = all_time_df[all_time_df['goal_user_name'] == user]['checkin_since_dt'].dropna()
                first_checkin = user_checkins_dates.min() if len(user_checkins_dates) > 0 else None
                last_checkin = user_checkins_dates.max() if len(user_checkins_dates) > 0 else None
                days_active = (last_checkin - first_checkin).days if first_checkin and last_checkin else 0
    
                checkin_frequency = user_total_checkins / weeks_in_quarter
                
                user_last_week_checkins = 0
                if len(user_checkins_dates) > 0:
                    last_week_checkins = user_checkins_dates[
                        (user_checkins_dates >= monday_last_week) & 
                        (user_checkins_dates <= sunday_last_week)
                    ]
                    user_last_week_checkins = len(last_week_checkins)
    
                overall_checkins.append({
                    'user_name': user,
                    'total_checkins': user_total_checkins,
                    'total_krs': user_total_krs,
                    'checkin_rate': checkin_rate,
                    'first_checkin': first_checkin,
                    'last_checkin': last_checkin,
                    'days_active': days_active,
                    'checkin_frequency_per_week': checkin_frequency,
                    'last_week_checkins': user_last_week_checkins,
                    'weeks_in_quarter': weeks_in_quarter
                })
            except Exception as e:
                st.warning(f"Error analyzing overall checkins for {user}: {e}")
                continue
    
        return sorted(overall_checkins, key=lambda x: x['total_checkins'], reverse=True)
        
    def get_last_month_end_date(self) -> datetime:
        """Get last day of previous month - always returns end of previous month regardless of current day"""
        today = datetime.now()
        
        # Get first day of current month
        first_day_current_month = datetime(today.year, today.month, 1)
        
        # Get last day of previous month
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        
        # Set time to end of day
        last_day_previous_month = last_day_previous_month.replace(hour=23, minute=59, second=59)
        
        return last_day_previous_month
    
    def should_calculate_monthly_shift(self) -> bool:
        """Check if monthly shift should be calculated (not in months 1,4,7,10)"""
        current_month = datetime.now().month
        return current_month not in [1, 4, 7, 10]
    
    def calculate_kr_shift_last_month(self, row: pd.Series, last_month_end: datetime) -> float:
        """Calculate kr_shift_last_month = kr_current_value - last_month_end_checkin_value
        Always compares against end of previous month"""
        try:
            # Get current kr value
            kr_current_value = pd.to_numeric(row.get('kr_current_value', 0), errors='coerce')
            if pd.isna(kr_current_value):
                kr_current_value = 0
            
            # Calculate last month end checkin value for this KR
            kr_id = row.get('kr_id', '')
            if not kr_id:
                return kr_current_value  # If no KR ID, shift = current value
            
            # Filter data for this specific KR and find checkins within range
            # FIX: Use DateUtils.get_quarter_start_date() instead of self.get_quarter_start_date()
            quarter_start = DateUtils.get_quarter_start_date()
            
            # Important: Use the reference month end as the cutoff
            reference_month_end = self.get_last_month_end_date()
            
            kr_checkins = self.final_df[
                (self.final_df['kr_id'] == kr_id) & 
                (self.final_df['checkin_id'].notna()) &
                (self.final_df['checkin_name'].notna()) &
                (self.final_df['checkin_name'] != '')
            ].copy()
            
            # Convert checkin dates and filter by time range up to reference month end
            if not kr_checkins.empty:
                kr_checkins['checkin_since_dt'] = pd.to_datetime(kr_checkins['checkin_since'], errors='coerce')
                kr_checkins = kr_checkins[
                    (kr_checkins['checkin_since_dt'] >= quarter_start) &
                    (kr_checkins['checkin_since_dt'] <= reference_month_end)
                ]
                
                # Get latest checkin value in range (up to reference month end)
                if not kr_checkins.empty:
                    latest_checkin = kr_checkins.loc[kr_checkins['checkin_since_dt'].idxmax()]
                    last_month_checkin_value = pd.to_numeric(latest_checkin.get('checkin_kr_current_value', 0), errors='coerce')
                    if pd.isna(last_month_checkin_value):
                        last_month_checkin_value = 0
                else:
                    last_month_checkin_value = 0
            else:
                last_month_checkin_value = 0
            
            # Calculate shift: current value - value as of reference month end
            kr_shift = kr_current_value - last_month_checkin_value
            return kr_shift
            
        except Exception as e:
            st.warning(f"Error calculating kr_shift_last_month: {e}")
            return 0
    
    def calculate_final_okr_goal_shift_monthly(self, user_df: pd.DataFrame) -> float:
        """
        Calculate final_okr_goal_shift using reference to previous month end:
        1. Group by unique combination of goal_name + kr_name
        2. Calculate average kr_shift_last_month for each combination
        3. Calculate average of all combination averages
        Always uses end of previous month as reference point
        """
        try:
            # Get reference month end
            reference_month_end = self.get_last_month_end_date()
            
            # Create unique combinations mapping
            unique_combinations = {}
            
            # Process each row to calculate kr_shift_last_month
            for idx, row in user_df.iterrows():
                goal_name = row.get('goal_name', '')
                kr_name = row.get('kr_name', '')
                
                # Skip rows without goal_name or kr_name
                if not goal_name or not kr_name:
                    continue
                
                # Create unique combination key
                combo_key = f"{goal_name}|{kr_name}"
                
                # Calculate kr_shift using reference month end
                kr_shift = self.calculate_kr_shift_last_month(row, reference_month_end)
                
                # Add to combinations
                if combo_key not in unique_combinations:
                    unique_combinations[combo_key] = []
                unique_combinations[combo_key].append(kr_shift)
            
            # Calculate final_okr_monthly_shift for each unique combination
            final_okr_monthly_shifts = []
            
            for combo_key, kr_shifts in unique_combinations.items():
                # Calculate average kr_shift_last_month for this combination
                if kr_shifts:
                    avg_kr_shift = sum(kr_shifts) / len(kr_shifts)
                    final_okr_monthly_shifts.append(avg_kr_shift)
            
            # Calculate final_okr_goal_shift_monthly (average of all final_okr_monthly_shift)
            if final_okr_monthly_shifts:
                final_okr_goal_shift_monthly = sum(final_okr_monthly_shifts) / len(final_okr_monthly_shifts)
            else:
                final_okr_goal_shift_monthly = 0
            
            return final_okr_goal_shift_monthly
            
        except Exception as e:
            st.error(f"Error calculating final_okr_goal_shift_monthly: {e}")
            return 0
    
    def calculate_last_month_value(self, last_month_end: datetime, df: pd.DataFrame = None) -> Tuple[float, List[Dict]]:
        """Calculate OKR value as of last month end"""
        if df is None:
            df = self.final_df
    
        try:
            df = df.copy()
            df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')
    
            unique_krs = df['kr_id'].dropna().unique()
            goal_values_dict = {}
            kr_details = []
    
            for kr_id in unique_krs:
                kr_data = df[df['kr_id'] == kr_id].copy()
                kr_data['checkin_since_dt'] = pd.to_datetime(kr_data['checkin_since'], errors='coerce')
    
                actual_checkins_before_month_end = kr_data[
                    (kr_data['checkin_since_dt'] <= last_month_end) &
                    (kr_data['checkin_name'].notna()) &
                    (kr_data['checkin_name'] != '')
                ]
    
                goal_name = kr_data.iloc[0]['goal_name'] if len(kr_data) > 0 else f"Unknown_{kr_id}"
    
                if len(actual_checkins_before_month_end) > 0:
                    latest_checkin_before_month_end = actual_checkins_before_month_end.sort_values('checkin_since_dt').iloc[-1]
                    kr_value = pd.to_numeric(latest_checkin_before_month_end['checkin_kr_current_value'], errors='coerce')
    
                    if pd.isna(kr_value):
                        kr_value = 0
    
                    if goal_name not in goal_values_dict:
                        goal_values_dict[goal_name] = []
                    goal_values_dict[goal_name].append(kr_value)
    
                    kr_details.append({
                        'kr_id': kr_id,
                        'goal_name': goal_name,
                        'kr_value': kr_value,
                        'checkin_date': latest_checkin_before_month_end['checkin_since_dt'],
                        'source': 'checkin_before_month_end'
                    })
                else:
                    kr_value = 0
                    goal_key = f"{goal_name}_no_checkin_{kr_id}"
                    goal_values_dict[goal_key] = [kr_value]
    
                    kr_details.append({
                        'kr_id': kr_id,
                        'goal_name': goal_name,
                        'kr_value': kr_value,
                        'checkin_date': None,
                        'source': 'no_checkin_before_month_end'
                    })
    
            goal_values = []
            for goal_name, kr_values_list in goal_values_dict.items():
                goal_value = np.mean(kr_values_list)
                goal_values.append(goal_value)
    
            last_month_value = np.mean(goal_values) if goal_values else 0
            return last_month_value, kr_details
    
        except Exception as e:
            st.error(f"Error calculating last month value: {e}")
            return 0, []

    def calculate_okr_shifts_by_user_monthly(self) -> List[Dict]:
        """Calculate monthly OKR shifts for each user always comparing against previous month end
        If shift > current_value, then shift = current_value - last_month_value"""
        try:
            if not self.should_calculate_monthly_shift():
                return []  # Return empty list if not applicable
                
            users = self.final_df['goal_user_name'].dropna().unique()
            user_okr_shifts_monthly = []
    
            # Get reference month end for all calculations
            reference_month_end = self.get_last_month_end_date()
    
            for user in users:
                user_df = self.final_df[self.final_df['goal_user_name'] == user].copy()
                
                # Calculate final_okr_goal_shift_monthly using reference month end
                final_okr_goal_shift_monthly = self.calculate_final_okr_goal_shift_monthly(user_df)
                
                # FIX: Use self.okr_calculator.calculate_current_value() instead of self.calculate_current_value()
                current_value = self.okr_calculator.calculate_current_value(user_df)
                last_month_value, kr_details = self.calculate_last_month_value(reference_month_end, user_df)
                legacy_okr_shift = current_value - last_month_value
    
                # NEW LOGIC: If shift > current_value, then shift = current_value - last_month_value
                adjusted_okr_shift = final_okr_goal_shift_monthly
                adjustment_applied = False
                
                if final_okr_goal_shift_monthly > current_value:
                    adjusted_okr_shift = current_value - last_month_value
                    adjustment_applied = True
    
                user_okr_shifts_monthly.append({
                    'user_name': user,
                    'okr_shift_monthly': adjusted_okr_shift,  # Use adjusted value
                    'original_shift_monthly': final_okr_goal_shift_monthly,  # Keep original for reference
                    'current_value': current_value,
                    'last_month_value': last_month_value,
                    'legacy_okr_shift_monthly': legacy_okr_shift,  # Keep old method for reference
                    'adjustment_applied': adjustment_applied,  # Flag to show if adjustment was applied
                    'kr_details_count': len(kr_details),
                    'reference_month_end': reference_month_end.strftime('%d/%m/%Y')  # Add reference date
                })
    
            # Sort by adjusted okr_shift_monthly descending
            return sorted(user_okr_shifts_monthly, key=lambda x: x['okr_shift_monthly'], reverse=True)
    
        except Exception as e:
            st.error(f"Error calculating monthly OKR shifts: {e}")
            return []



# ==================== STREAMLIT UI FUNCTIONS ====================

# Function to generate data table
def generate_data_table(users):
    # Create a DataFrame from users
    data = []
    for user in users:
        data.append({
            "Name": user.name,
            "Has OKR": "Yes" if user.co_OKR == 1 else "No",
            "Check-in": "Yes" if user.checkin == 1 else "No",
            "OKR Movement": user.dich_chuyen_OKR,
            "Score": user.score
        })
    
    df = pd.DataFrame(data)
    return df

# Add this function to your file
def export_to_excel(users, filename="output1.xlsx"):
    """
    Xuất dữ liệu OKRs của danh sách users ra file Excel với giao diện được cải tiến.

    Yêu cầu:
      - Mỗi user phải có các thuộc tính: name, co_OKR, checkin, dich_chuyen_OKR, score
    """
    # Tạo workbook và sheet
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "OKRs"

    # Định nghĩa style
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    category_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    category_font = Font(bold=True)
    thin_border = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))

    # --- Tiêu đề chính ---
    total_columns = 3 + len(users)  # 3 cột cố định + số user
    last_col_letter = get_column_letter(total_columns)
    ws.merge_cells(f"A1:{last_col_letter}1")
    title_cell = ws["A1"]
    title_cell.value = "ĐÁNH GIÁ OKRs THÁNG"
    title_cell.font = Font(size=14, bold=True)
    title_cell.alignment = Alignment(horizontal="center", vertical="center")

    # --- Header (dòng 2) ---
    fixed_headers = ["TT", "Nội dung", "Tự chấm điểm"]
    user_headers = [user.name for user in users]
    headers = fixed_headers + user_headers
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=2, column=col_idx, value=header)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = thin_border
        # Đặt độ rộng mặc định cho các cột
        col_letter = get_column_letter(col_idx)
        if col_idx == 2:
            ws.column_dimensions[col_letter].width = 70  # Nội dung dài hơn
        elif col_idx == 1:
            ws.column_dimensions[col_letter].width = 5
        else:
            ws.column_dimensions[col_letter].width = 15

    # --- Các dòng tiêu chí (bắt đầu từ dòng 3) ---
    criteria = [
        [1, "Đầy đủ OKRs cá nhân được cập nhật trên Base Goal (Mục tiêu cá nhân + Đường dẫn)", 1],
        [2, "Có Check-in trên base hàng tuần (Mỗi tuần ít nhất 1 lần check-in)", 0.5],
        [3, "Có check-in với người khác, cấp quản lý, làm việc chung OKRs trong bộ phận", 0.5],
        [4, "Tổng OKRs dịch chuyển trong tháng (so với tháng trước):", ""],
        ["", "Nhỏ hơn 10%", 0.15],
        ["", "Từ 10 - 25%", 0.25],
        ["", "Từ 26 - 30%", 0.5],
        ["", "Từ 31 - 50%", 0.75],
        ["", "Từ 51 - 80%", 1.25],
        ["", "Từ 81% - 99%", 1.5],
        ["", "100% hoặc có đột phá lớn", 2.5],
        [5, "Tổng cộng OKRs", ""]
    ]
    start_row = 3
    for i, row_data in enumerate(criteria):
        row_idx = start_row + i
        for col_idx, value in enumerate(row_data, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = thin_border
            # Đánh dấu cột loại (nếu giá trị đầu tiên là số thứ tự) với màu nền và in đậm
            if col_idx == 1 and isinstance(value, int):
                cell.fill = category_fill
                cell.font = category_font

    # --- Ghi dữ liệu của từng user ---
    # Các user sẽ được hiển thị từ cột 4 trở đi
    for idx, user in enumerate(users, start=1):
        col_idx = 3 + idx  # cột thứ 1-3 đã dành cho tiêu đề cố định
        col_letter = get_column_letter(col_idx)
        # 1. Đánh giá OKRs cá nhân (dòng 3)
        ws.cell(row=3, column=col_idx, value=1 if user.co_OKR == 1 else 0)
        # 2. Check-in hàng tuần (dòng 4)
        ws.cell(row=4, column=col_idx, value=0.5 if user.checkin == 1 else 0)
        # 3. Check-in với người khác (dòng 5)
        ws.cell(row=5, column=col_idx, value=0.5 )

        # 4. Dịch chuyển OKR:
        # Dòng 6 hiển thị % dịch chuyển, các dòng từ 7 đến 13 hiển thị điểm tương ứng
        movement = user.dich_chuyen_OKR
        ws.cell(row=6, column=col_idx, value=f"{movement}%")

        # Xác định điểm dịch chuyển dựa theo % và dòng ghi điểm:
        if movement < 10:
            score_value = 0.15
            movement_row = 7
        elif movement < 26:
            score_value = 0.25
            movement_row = 8
        elif movement < 31:
            score_value = 0.5
            movement_row = 9
        elif movement < 51:
            score_value = 0.75
            movement_row = 10
        elif movement < 81:
            score_value = 1.25
            movement_row = 11
        elif movement < 100:
            score_value = 1.5
            movement_row = 12
        else:
            score_value = 2.5
            movement_row = 13
        ws.cell(row=movement_row, column=col_idx, value=score_value)

        # 5. Tổng điểm: sử dụng công thức SUM từ dòng 3 đến dòng 13
        formula = user.score
        ws.cell(row=14, column=col_idx, value=formula)

        # Áp dụng border và căn giữa cho các ô dữ liệu của user
        for r in range(3, 15):
            cell = ws.cell(row=r, column=col_idx)
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = thin_border

    # --- Freeze panes để cố định header và tiêu chí ---
    ws.freeze_panes = ws["D3"]

    # --- Tự động điều chỉnh độ rộng cột (nếu cần) ---
    # Vòng lặp qua các cột để tính độ rộng dựa trên nội dung
    for col in ws.columns:
        max_length = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        adjusted_width = max_length + 2
        ws.column_dimensions[col_letter].width = adjusted_width

    # Return the workbook object
    return wb


def show_data_summary(df, analyzer):
    """Show data summary statistics"""
    st.subheader("📈 Data Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_goals = df['goal_id'].nunique()
        st.metric("Total Goals", total_goals)
    
    with col2:
        total_krs = df['kr_id'].nunique()
        st.metric("Total KRs", total_krs)
    
    with col3:
        total_checkins = df['checkin_id'].nunique()
        st.metric("Total Checkins", total_checkins)
    
    with col4:
        total_users = df['goal_user_name'].nunique()
        st.metric("Total Users", total_users)
    
    with col5:
        total_filtered_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
        st.metric("Filtered Members", total_filtered_members)

def show_missing_analysis_section(analyzer):
    """Show missing goals and checkins analysis"""
    
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
            
            # Download button for members without goals
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
            st.warning("These members have set up goals but haven't made any checkins yet. They may need guidance or reminders.")
            
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

def show_okr_analysis(okr_shifts, reference_date, period="weekly"):
    """Show OKR shift analysis with reference date"""
    
    period_label = "tuần" if period == "weekly" else "tháng"
    shift_key = 'okr_shift' if period == "weekly" else 'okr_shift_monthly'
    last_value_key = 'last_friday_value' if period == "weekly" else 'last_month_value'
    
    # Display reference information
    reference_label = f"thứ 6 {period_label} trước" if period == "weekly" else f"cuối {period_label} trước"
    st.info(f"📅 **Ngày tham chiếu:** {reference_label.title()} ({reference_date.strftime('%d/%m/%Y')})")
    st.info(f"📊 **Logic tính toán:** So sánh giá trị hiện tại với giá trị tại {reference_label}")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    progress_users = len([u for u in okr_shifts if u[shift_key] > 0])
    stable_users = len([u for u in okr_shifts if u[shift_key] == 0])
    issue_users = len([u for u in okr_shifts if u[shift_key] < 0])
    avg_shift = np.mean([u[shift_key] for u in okr_shifts])
    
    with col1:
        st.metric("Tiến bộ", progress_users, delta=f"{progress_users/len(okr_shifts)*100:.1f}%")
    
    with col2:
        st.metric("Ổn định", stable_users, delta=f"{stable_users/len(okr_shifts)*100:.1f}%")
    
    with col3:
        st.metric("Cần hỗ trợ", issue_users, delta=f"{issue_users/len(okr_shifts)*100:.1f}%")
    
    with col4:
        st.metric("Dịch chuyển TB", f"{avg_shift:.2f}", delta=None)
    
    # OKR shift chart with reference date in title
    okr_df = pd.DataFrame(okr_shifts)
    
    fig = px.bar(
        okr_df.head(20), 
        x='user_name', 
        y=shift_key,
        title=f"Dịch chuyển OKR so với {reference_label} ({reference_date.strftime('%d/%m/%Y')})",
        color=shift_key,
        color_continuous_scale=['red', 'yellow', 'green'],
        labels={
            'user_name': 'Nhân viên',
            shift_key: f'Dịch chuyển OKR ({period_label})'
        }
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Top performers table
    st.subheader(f"🏆 Nhân viên tiến bộ nhất ({period_label})")
    top_performers = okr_df[okr_df[shift_key] > 0].head(10)
    if not top_performers.empty:
        display_cols = ['user_name', shift_key, 'current_value', last_value_key]
        display_df = top_performers[display_cols].round(2)
        display_df.columns = ['Nhân viên', f'Dịch chuyển ({period_label})', 'Giá trị hiện tại', f'Giá trị {reference_label}']
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info(f"Không có nhân viên nào có dịch chuyển OKR {period_label} dương")
    
    # Issues table
    if issue_users > 0:
        st.subheader(f"⚠️ Nhân viên cần hỗ trợ ({period_label})")
        issue_df = okr_df[okr_df[shift_key] < 0]
        display_cols = ['user_name', shift_key, 'current_value', last_value_key]
        display_df = issue_df[display_cols].round(2)
        display_df.columns = ['Nhân viên', f'Dịch chuyển ({period_label})', 'Giá trị hiện tại', f'Giá trị {reference_label}']
        st.dataframe(display_df, use_container_width=True, hide_index=True)

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
    
    today = datetime.now()
    days_since_monday = today.weekday()
    monday_this_week = today - timedelta(days=days_since_monday)
    monday_last_week = monday_this_week - timedelta(days=7)
    sunday_last_week = monday_last_week + timedelta(days=6)
    
    weeks_in_quarter = (today - quarter_start).days / 7
    weeks_in_quarter = max(weeks_in_quarter, 1)
    
    st.info(f"📅 Tuần trước: {monday_last_week.strftime('%d/%m/%Y')} - {sunday_last_week.strftime('%d/%m/%Y')}")
    st.info(f"📊 Tần suất checkin = Tổng checkin ÷ {weeks_in_quarter:.1f} tuần (từ đầu quý đến nay)")
    
    # Select and format columns for display
    top_overall = overall_df.nlargest(20, 'total_checkins').copy()
    
    display_df = top_overall[[
        'user_name', 
        'total_checkins', 
        'checkin_frequency_per_week',
        'last_week_checkins'
    ]].copy()
    
    display_df.columns = [
        '👤 Nhân viên',
        '📊 Tổng checkin', 
        '⚡ Tần suất/tuần (quý)',
        '📅 Checkin tuần trước'
    ]
    
    display_df['⚡ Tần suất/tuần (quý)'] = display_df['⚡ Tần suất/tuần (quý)'].round(2)
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "👤 Nhân viên": st.column_config.TextColumn("👤 Nhân viên", width="medium"),
            "📊 Tổng checkin": st.column_config.NumberColumn("📊 Tổng checkin", width="small"),
            "⚡ Tần suất/tuần (quý)": st.column_config.NumberColumn("⚡ Tần suất/tuần (quý)", format="%.2f", width="medium"),
            "📅 Checkin tuần trước": st.column_config.NumberColumn("📅 Checkin tuần trước", width="small")
        }
    )
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_last_week = overall_df['last_week_checkins'].sum()
        st.metric("🗓️ Tổng checkin tuần trước", total_last_week)
    
    with col2:
        active_last_week = len(overall_df[overall_df['last_week_checkins'] > 0])
        st.metric("👥 Người hoạt động tuần trước", active_last_week)
    
    with col3:
        avg_frequency_quarter = overall_df['checkin_frequency_per_week'].mean()
        st.metric("📈 Tần suất TB/tuần (quý)", f"{avg_frequency_quarter:.2f}")
    
    with col4:
        max_frequency_quarter = overall_df['checkin_frequency_per_week'].max()
        st.metric("🏆 Tần suất cao nhất/tuần", f"{max_frequency_quarter:.2f}")
    
    # Frequency distribution chart
    st.subheader("📈 Phân bố tần suất checkin theo tuần")
    
    frequency_data = overall_df['checkin_frequency_per_week'].dropna()
    
    fig_freq = go.Figure()
    fig_freq.add_trace(go.Histogram(
        x=frequency_data, 
        nbinsx=15, 
        name="Frequency Distribution",
        marker_color='lightblue',
        opacity=0.7
    ))
    fig_freq.update_layout(
        title=f"Phân bố tần suất checkin/tuần (Tính theo {weeks_in_quarter:.1f} tuần trong quý)",
        xaxis_title="Số checkin/tuần",
        yaxis_title="Số nhân viên",
        height=400
    )
    
    # Add average line
    avg_line = avg_frequency_quarter
    fig_freq.add_vline(
        x=avg_line,
        line_dash="dash",
        line_color="red",
        annotation_text=f"TB: {avg_line:.2f}"
    )
    
    st.plotly_chart(fig_freq, use_container_width=True)

def show_export_options(df, okr_shifts, okr_shifts_monthly, period_checkins, overall_checkins, analyzer):
    """Show data export options including monthly data"""
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        if st.button("📊 Export Full Dataset"):
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"okr_full_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col2:
        if st.button("🎯 Export Weekly OKR Shifts"):
            csv = pd.DataFrame(okr_shifts).to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"okr_shifts_weekly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col3:
        if okr_shifts_monthly and st.button("🗓️ Export Monthly OKR Shifts"):
            csv = pd.DataFrame(okr_shifts_monthly).to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"okr_shifts_monthly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col4:
        if st.button("📝 Export Period Checkins"):
            csv = pd.DataFrame(period_checkins).to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"period_checkins_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col5:
        if st.button("📈 Export Overall Checkins"):
            csv = pd.DataFrame(overall_checkins).to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"overall_checkins_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col6:
        if st.button("👥 Export Filtered Members"):
            if analyzer.filtered_members_df is not None:
                csv = analyzer.filtered_members_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"filtered_members_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

def show_okr_analysis_monthly(okr_shifts_monthly, last_month_end):
    """Show monthly OKR shift analysis with reference date"""
    
    # Display reference information
    st.info(f"📅 **Ngày tham chiếu:** Cuối tháng trước ({last_month_end.strftime('%d/%m/%Y')})")
    st.info(f"📊 **Logic tính toán:** So sánh giá trị hiện tại với giá trị tại cuối tháng trước")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    progress_users = len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] > 0])
    stable_users = len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] == 0])
    issue_users = len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] < 0])
    avg_shift = np.mean([u['okr_shift_monthly'] for u in okr_shifts_monthly])
    
    with col1:
        st.metric("Tiến bộ", progress_users, delta=f"{progress_users/len(okr_shifts_monthly)*100:.1f}%")
    
    with col2:
        st.metric("Ổn định", stable_users, delta=f"{stable_users/len(okr_shifts_monthly)*100:.1f}%")
    
    with col3:
        st.metric("Cần hỗ trợ", issue_users, delta=f"{issue_users/len(okr_shifts_monthly)*100:.1f}%")
    
    with col4:
        st.metric("Dịch chuyển TB", f"{avg_shift:.2f}", delta=None)
    
    # OKR shift chart with reference date in title
    okr_df = pd.DataFrame(okr_shifts_monthly)
    
    fig = px.bar(
        okr_df.head(20), 
        x='user_name', 
        y='okr_shift_monthly',
        title=f"Dịch chuyển OKR so với cuối tháng trước ({last_month_end.strftime('%d/%m/%Y')})",
        color='okr_shift_monthly',
        color_continuous_scale=['red', 'yellow', 'green'],
        labels={
            'user_name': 'Nhân viên',
            'okr_shift_monthly': 'Dịch chuyển OKR (tháng)'
        }
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Top performers table
    st.subheader("🏆 Nhân viên tiến bộ nhất (tháng)")
    top_performers = okr_df[okr_df['okr_shift_monthly'] > 0].head(10)
    if not top_performers.empty:
        display_cols = ['user_name', 'okr_shift_monthly', 'current_value', 'last_month_value']
        display_df = top_performers[display_cols].round(2)
        display_df.columns = ['Nhân viên', 'Dịch chuyển (tháng)', 'Giá trị hiện tại', f'Giá trị cuối tháng trước']
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("Không có nhân viên nào có dịch chuyển OKR tháng dương")
    
    # Issues table
    if issue_users > 0:
        st.subheader("⚠️ Nhân viên cần hỗ trợ (tháng)")
        issue_df = okr_df[okr_df['okr_shift_monthly'] < 0]
        display_cols = ['user_name', 'okr_shift_monthly', 'current_value', 'last_month_value']
        display_df = issue_df[display_cols].round(2)
        display_df.columns = ['Nhân viên', 'Dịch chuyển (tháng)', 'Giá trị hiện tại', f'Giá trị cuối tháng trước']
        st.dataframe(display_df, use_container_width=True, hide_index=True)

def run_analysis(analyzer, selected_cycle, show_missing_analysis):
    """Run the main analysis"""
    
    st.header(f"📊 Analysis Results for {selected_cycle['name']}")
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(message, progress):
        status_text.text(message)
        progress_bar.progress(progress)
    
    # Load data
    try:
        df = analyzer.load_and_process_data(update_progress)
        
        if df is None or df.empty:
            st.error("❌ Failed to load data. Please check your API tokens and try again.")
            return
            
        progress_bar.empty()
        status_text.empty()
        
        # Show data summary
        show_data_summary(df, analyzer)
        
        # Show missing goals and checkins analysis if enabled
        if show_missing_analysis:
            st.subheader("🚨 Missing Goals & Checkins Analysis")
            with st.spinner("Analyzing missing goals and checkins..."):
                show_missing_analysis_section(analyzer)
        
        # NEW: User Score Analysis with Monthly OKR Calculation
        st.subheader("🏆 User Score Analysis (Monthly OKR Integration)")
        with st.spinner("Calculating user scores with monthly OKR movement..."):
            user_scores_df = show_user_score_analysis(analyzer)
        
        # Calculate OKR shifts (Weekly)
        st.subheader("🎯 Weekly OKR Shift Analysis")
        with st.spinner("Calculating weekly OKR shifts..."):
            okr_shifts = analyzer.calculate_okr_shifts_by_user()
        
        if okr_shifts:
            show_okr_analysis(okr_shifts, DateUtils.get_last_friday_date(), "weekly")
        else:
            st.warning("No weekly OKR shift data available")
        
        # Calculate OKR shifts (Monthly)
        if analyzer.should_calculate_monthly_shift():
            st.subheader("🗓️ Monthly OKR Shift Analysis")
            with st.spinner("Calculating monthly OKR shifts..."):
                okr_shifts_monthly = analyzer.calculate_okr_shifts_by_user_monthly()
            
            if okr_shifts_monthly:
                show_okr_analysis_monthly(okr_shifts_monthly, analyzer.get_last_month_end_date())
            else:
                st.warning("No monthly OKR shift data available")
        else:
            current_month = datetime.now().month
            quarter_months = {1: "Q1", 4: "Q2", 7: "Q3", 10: "Q4"}
            st.info(f"ℹ️ Monthly OKR shift analysis is not calculated for month {current_month} (start of {quarter_months.get(current_month, 'quarter')})")
        
        # Analyze checkin behavior
        st.subheader("📝 Checkin Behavior Analysis")
        with st.spinner("Analyzing checkin behavior..."):
            period_checkins, overall_checkins = analyzer.analyze_checkin_behavior()
        
        if period_checkins and overall_checkins:
            show_checkin_behavior(period_checkins, overall_checkins, DateUtils.get_last_friday_date(), DateUtils.get_quarter_start_date())
        else:
            st.warning("No checkin data available")
        
        # Data export - Updated to include monthly data
        st.subheader("💾 Export Data")
        okr_shifts_monthly = analyzer.calculate_okr_shifts_by_user_monthly() if analyzer.should_calculate_monthly_shift() else []
        show_export_options(df, okr_shifts, okr_shifts_monthly, period_checkins, overall_checkins, analyzer)
        
        st.success("✅ Analysis completed successfully!")
        
    except Exception as e:
        st.error(f"❌ Analysis failed: {e}")
        progress_bar.empty()
        status_text.empty()

def send_email_report(analyzer, email_generator, selected_cycle, email_from, email_password, email_to):
    """Send email report including monthly data when applicable"""
    
    st.header("📧 Sending Email Report")
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(message, progress):
        status_text.text(message)
        progress_bar.progress(progress)
    
    try:
        # Load and process data
        update_progress("Loading data for email report...", 0.1)
        df = analyzer.load_and_process_data(update_progress)
        
        if df is None or df.empty:
            st.error("❌ Failed to load data for email report")
            return
        
        update_progress("Analyzing missing goals and checkins...", 0.25)
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        
        update_progress("Calculating weekly OKR shifts...", 0.4)
        okr_shifts = analyzer.calculate_okr_shifts_by_user()
        
        # Calculate monthly OKR shifts if applicable
        okr_shifts_monthly = []
        if analyzer.should_calculate_monthly_shift():
            update_progress("Calculating monthly OKR shifts...", 0.55)
            okr_shifts_monthly = analyzer.calculate_okr_shifts_by_user_monthly()
        
        update_progress("Creating email content...", 0.7)
        html_content = email_generator.create_email_content(
            analyzer, selected_cycle, members_without_goals, members_without_checkins,
            members_with_goals_no_checkins, okr_shifts, okr_shifts_monthly
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
            monthly_note = " (bao gồm phân tích tháng)" if okr_shifts_monthly else ""
            st.info(f"📧 Email report sent to: {email_to}{monthly_note}")
            
            # Show email preview
            if st.checkbox("📋 Show email preview", value=False):
                st.subheader("Email Preview")
                st.components.v1.html(html_content, height=600, scrolling=True)
        else:
            st.error(f"❌ {message}")
            
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"❌ Error sending email report: {e}")

def main():
    """Main application entry point"""
    st.title("🎯 OKR & Checkin Analysis Dashboard")
    st.markdown("---")

    # Get API tokens from environment variables
    goal_token = os.getenv("GOAL_ACCESS_TOKEN")
    account_token = os.getenv("ACCOUNT_ACCESS_TOKEN")

    # Check if tokens are available
    if not goal_token or not account_token:
        st.error("❌ API tokens not found in environment variables. Please set GOAL_ACCESS_TOKEN and ACCOUNT_ACCESS_TOKEN.")
        st.info("Make sure to set the following environment variables:")
        st.code("""
GOAL_ACCESS_TOKEN=your_goal_token_here
ACCOUNT_ACCESS_TOKEN=your_account_token_here
        """)
        return

    # Initialize analyzer
    try:
        analyzer = OKRAnalysisSystem(goal_token, account_token)
        email_generator = EmailReportGenerator()
    except Exception as e:
        st.error(f"Failed to initialize analyzer: {e}")
        return

    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Show token status
        st.subheader("🔑 API Token Status")
        st.success("✅ Goal Access Token: Loaded")
        st.success("✅ Account Access Token: Loaded")

    # Get cycles
    with st.spinner("🔄 Loading available cycles..."):
        cycles = analyzer.get_cycle_list()

    if not cycles:
        st.error("❌ Could not load cycles. Please check your API tokens and connection.")
        return

    # Cycle selection
    with st.sidebar:
        st.subheader("📅 Cycle Selection")
        cycle_options = {f"{cycle['name']} ({cycle['formatted_start_time']})": cycle for cycle in cycles}
        selected_cycle_name = st.selectbox(
            "Select Cycle",
            options=list(cycle_options.keys()),
            index=0,
            help="Choose the quarterly cycle to analyze"
        )
        
        selected_cycle = cycle_options[selected_cycle_name]
        analyzer.checkin_path = selected_cycle['path']
        
        st.info(f"🎯 **Selected Cycle:**\n\n**{selected_cycle['name']}**\n\nPath: `{selected_cycle['path']}`\n\nStart: {selected_cycle['formatted_start_time']}")

    # Analysis options
    with st.sidebar:
        st.subheader("📊 Analysis Options")
        show_missing_analysis = st.checkbox("Show Missing Goals & Checkins Analysis", value=True)

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

    # Main analysis
    col1, col2 = st.columns(2)
    
    with col1:
        analyze_button = st.button("🚀 Start Analysis", type="primary", use_container_width=True)
    
    with col2:
        email_button = st.button("📧 Send Email Report", type="secondary", use_container_width=True)

    if analyze_button:
        run_analysis(analyzer, selected_cycle, show_missing_analysis)

    # Send email report
    if email_button:
        send_email_report(analyzer, email_generator, selected_cycle, email_from, email_password, email_to)

if __name__ == "__main__":
    main()

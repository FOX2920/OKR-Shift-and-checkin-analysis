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

# Constants
QUARTER_START_MONTHS = [1, 4, 7, 10]
MIN_WEEKLY_CHECKINS = 3
REQUEST_TIMEOUT = 30
MAX_PAGES_KRS = 50
MAX_PAGES_CHECKINS = 100

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
    def get_last_month_end_date() -> datetime:
        """Get last day of previous month"""
        today = datetime.now()
        first_day_current_month = datetime(today.year, today.month, 1)
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        return last_day_previous_month.replace(hour=23, minute=59, second=59)

    @staticmethod
    def convert_timestamp_to_datetime(timestamp) -> Optional[str]:
        """Convert timestamp to datetime string"""
        if timestamp is None or timestamp == '' or timestamp == 0:
            return None
        try:
            return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return None

    @staticmethod
    def should_calculate_monthly_shift() -> bool:
        """Check if monthly shift should be calculated (not in months 1,4,7,10)"""
        return datetime.now().month not in QUARTER_START_MONTHS


class User:
    """User class for OKR tracking"""
    
    def __init__(self, user_id, name, co_OKR=1, checkin=0, dich_chuyen_OKR=0, score=0):
        self.user_id = str(user_id)
        self.name = name
        self.co_OKR = co_OKR
        self.checkin = checkin
        self.dich_chuyen_OKR = dich_chuyen_OKR
        self.score = score
        self.OKR = {month: 0 for month in range(1, 13)}

    def update_okr(self, month, value):
        """Update OKR for specific month"""
        if 1 <= month <= 12:
            self.OKR[month] = value

    def calculate_score(self):
        """Calculate score based on criteria: check-in, OKR and OKR movement"""
        score = 0.5
        
        # Check-in contributes 0.5 points
        if self.checkin == 1:
            score += 0.5
        
        # Having OKR contributes 1 point
        if self.co_OKR == 1:
            score += 1
        
        # OKR movement scoring
        movement = self.dich_chuyen_OKR
        movement_scores = [
            (10, 0.15), (25, 0.25), (30, 0.5), (50, 0.75),
            (80, 1.25), (99, 1.5), (float('inf'), 2.5)
        ]
        
        for threshold, points in movement_scores:
            if movement < threshold:
                score += points
                break
        
        self.score = round(score, 2)

    def __repr__(self):
        return (f"User(id={self.user_id}, name={self.name}, co_OKR={self.co_OKR}, "
                f"checkin={self.checkin}, dich_chuyen_OKR={self.dich_chuyen_OKR}, score={self.score})")


class UserManager:
    """Manages user data and calculations"""
    
    def __init__(self, account_df, krs_df, checkin_df, cycle_df=None, final_df=None):
        self.account_df = account_df
        self.krs_df = krs_df
        self.checkin_df = checkin_df
        self.cycle_df = cycle_df
        self.final_df = final_df
        
        self.user_name_map = self._create_user_name_map()
        self.users = self._create_users()

    def _create_user_name_map(self) -> Dict[str, str]:
        """Create user_id to name mapping from account_df"""
        user_map = {}
        if not self.account_df.empty and 'id' in self.account_df.columns and 'name' in self.account_df.columns:
            for _, row in self.account_df.iterrows():
                user_map[str(row['id'])] = row.get('name', 'Unknown')
        return user_map

    def _create_users(self) -> Dict[str, User]:
        """Create User objects from KRs data"""
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
        """Update check-in status for each user"""
        for user in self.users.values():
            if self._has_weekly_checkins(user.user_id, start_date, end_date):
                user.checkin = 1

    def _has_weekly_checkins(self, user_id, start_date=None, end_date=None) -> bool:
        """Check if user has checkins in at least 3 weeks within specified period"""
        if start_date is None:
            start_date = DateUtils.get_quarter_start_date().date()
        if end_date is None:
            end_date = date.today()
            
        start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_datetime = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        
        checkins = self._get_user_checkins(user_id)
        checkins_in_range = [dt for dt in checkins if start_datetime <= dt <= end_datetime]
        
        if not checkins_in_range:
            return False
        
        weekly_checkins = set(dt.isocalendar()[1] for dt in checkins_in_range)
        return len(weekly_checkins) >= MIN_WEEKLY_CHECKINS

    def _get_user_checkins(self, user_id) -> List[datetime]:
        """Get all checkin dates for a user"""
        checkins = []
        if not self.checkin_df.empty and 'user_id' in self.checkin_df.columns and 'day' in self.checkin_df.columns:
            user_checkins = self.checkin_df[self.checkin_df['user_id'].astype(str) == str(user_id)]
            for _, entry in user_checkins.iterrows():
                try:
                    checkin_date = datetime.fromtimestamp(float(entry.get('day')), tz=timezone.utc)
                    checkins.append(checkin_date)
                except (ValueError, TypeError):
                    continue
        return checkins

    def update_okr_movement(self):
        """Update OKR movement for each user"""
        if not DateUtils.should_calculate_monthly_shift():
            self._update_okr_movement_quarter_start()
        else:
            self._update_okr_movement_monthly()

    def _update_okr_movement_quarter_start(self):
        """Update OKR movement for quarter start months (1,4,7,10)"""
        for user in self.users.values():
            current_okr = self._calculate_current_value_for_user(user.user_id)
            user.dich_chuyen_OKR = current_okr

    def _update_okr_movement_monthly(self):
        """Update OKR movement for non-quarter start months"""
        for user in self.users.values():
            user_id = user.user_id
            current_okr = self._calculate_current_value_for_user(user_id)
            monthly_shift = self._calculate_final_okr_goal_shift_monthly_for_user(user_id)
            
            if self.final_df is not None:
                user_name = self.user_name_map.get(user_id, '')
                user_df = self.final_df[self.final_df['goal_user_name'] == user_name].copy()
                
                if not user_df.empty:
                    last_month_end = DateUtils.get_last_month_end_date()
                    last_month_value = self._calculate_last_month_value_for_user(user_df, last_month_end)
                    
                    # Áp dụng logic mới theo yêu cầu:
                    # 1. Nếu giá trị cuối tháng trước > giá trị hiện tại thì giá trị cuối tháng = giá trị hiện tại - dịch chuyển tháng
                    # 2. Nếu giá trị cuối tháng trước < giá trị hiện tại và (giá trị hiện tại - giá trị cuối tháng trước) != dịch chuyển
                    #    thì dịch chuyển tháng = giá trị hiện tại - giá trị cuối tháng trước
                    
                    final_shift = monthly_shift
                    
                    # Quy tắc 1: Nếu last_month_value > current_okr
                    if last_month_value > current_okr:
                        # Điều chỉnh reference value: last_month_value = current_okr - monthly_shift
                        adjusted_last_month_value = current_okr - monthly_shift
                        final_shift = monthly_shift  # Giữ nguyên shift
                    
                    # Quy tắc 2: Nếu last_month_value < current_okr VÀ (current_okr - last_month_value) != monthly_shift
                    elif last_month_value < current_okr and (current_okr - last_month_value) != monthly_shift:
                        final_shift = current_okr - last_month_value
                    
                    user.dich_chuyen_OKR = round(final_shift, 2)
                else:
                    user.dich_chuyen_OKR = round(monthly_shift, 2)
            else:
                user.dich_chuyen_OKR = round(monthly_shift, 2)

    def _calculate_current_value_for_user(self, user_id) -> float:
        """Calculate current OKR value for a specific user"""
        try:
            if self.final_df is None:
                return 0
            
            user_name = self.user_name_map.get(user_id, '')
            if not user_name:
                return 0
                
            user_df = self.final_df[self.final_df['goal_user_name'] == user_name].copy()
            if user_df.empty:
                return 0
                
            unique_goals = user_df.groupby('goal_name')['goal_current_value'].first().reset_index()
            unique_goals['goal_current_value'] = pd.to_numeric(unique_goals['goal_current_value'], errors='coerce').fillna(0)
            return unique_goals['goal_current_value'].mean() if len(unique_goals) > 0 else 0
            
        except Exception as e:
            st.error(f"Error calculating current value for user {user_id}: {e}")
            return 0

    def _calculate_final_okr_goal_shift_monthly_for_user(self, user_id) -> float:
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
            
            reference_month_end = DateUtils.get_last_month_end_date()
            unique_combinations = {}
            
            for _, row in user_df.iterrows():
                goal_name = row.get('goal_name', '')
                kr_name = row.get('kr_name', '')
                
                if not goal_name or not kr_name:
                    continue
                
                combo_key = f"{goal_name}|{kr_name}"
                kr_shift = self._calculate_kr_shift_last_month(row, reference_month_end)
                
                if combo_key not in unique_combinations:
                    unique_combinations[combo_key] = []
                unique_combinations[combo_key].append(kr_shift)
            
            final_okr_monthly_shifts = [
                sum(kr_shifts) / len(kr_shifts) 
                for kr_shifts in unique_combinations.values() 
                if kr_shifts
            ]
            
            return sum(final_okr_monthly_shifts) / len(final_okr_monthly_shifts) if final_okr_monthly_shifts else 0
            
        except Exception as e:
            st.error(f"Error calculating final_okr_goal_shift_monthly for user {user_id}: {e}")
            return 0

    def _calculate_kr_shift_last_month(self, row, reference_month_end) -> float:
        """Calculate kr_shift_last_month = kr_current_value - last_month_end_checkin_value"""
        try:
            kr_current_value = pd.to_numeric(row.get('kr_current_value', 0), errors='coerce')
            if pd.isna(kr_current_value):
                kr_current_value = 0
            
            kr_id = row.get('kr_id', '')
            if not kr_id or self.final_df is None:
                return kr_current_value
            
            quarter_start = DateUtils.get_quarter_start_date()
            
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
            
            return kr_current_value - last_month_checkin_value
            
        except Exception as e:
            st.warning(f"Error calculating kr_shift_last_month: {e}")
            return 0

    def _calculate_last_month_value_for_user(self, user_df, last_month_end) -> float:
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
                    latest_checkin = actual_checkins_before_month_end.sort_values('checkin_since_dt').iloc[-1]
                    kr_value = pd.to_numeric(latest_checkin['checkin_kr_current_value'], errors='coerce')
                    kr_value = kr_value if not pd.isna(kr_value) else 0

                    if goal_name not in goal_values_dict:
                        goal_values_dict[goal_name] = []
                    goal_values_dict[goal_name].append(kr_value)
                else:
                    goal_key = f"{goal_name}_no_checkin_{kr_id}"
                    goal_values_dict[goal_key] = [0]

            goal_values = [np.mean(kr_values_list) for kr_values_list in goal_values_dict.values()]
            return np.mean(goal_values) if goal_values else 0

        except Exception as e:
            st.error(f"Error calculating last month value: {e}")
            return 0

    def calculate_scores(self):
        """Calculate scores for all users"""
        for user in self.users.values():
            user.calculate_score()

    def get_users(self) -> List[User]:
        """Return list of all users"""
        return list(self.users.values())


class APIClient:
    """Client for handling API requests"""
    
    def __init__(self, goal_token: str, account_token: str):
        self.goal_token = goal_token
        self.account_token = account_token

    def _make_request(self, url: str, data: Dict, description: str = "") -> requests.Response:
        """Make HTTP request with error handling"""
        try:
            response = requests.post(url, data=data, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            st.error(f"Error {description}: {e}")
            raise

    def get_filtered_members(self) -> pd.DataFrame:
        """Get filtered members from account API"""
        url = "https://account.base.vn/extapi/v1/group/get"
        data = {"access_token": self.account_token, "path": "aplus"}
        
        response = self._make_request(url, data, "fetching account members")
        response_data = response.json()
        
        members = response_data.get('group', {}).get('members', [])
        
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
        
        return self._apply_member_filters(df)

    def _apply_member_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply filters to member dataframe"""
        excluded_jobs = 'kcs|agile|khu vực|sa ti co|trainer|specialist|no|chuyên gia|xnk|vat|trưởng phòng thị trường'
        filtered_df = df[~df['job'].str.lower().str.contains(excluded_jobs, na=False)]
        return filtered_df[filtered_df['username'] != 'ThuAn']

    def get_cycle_list(self) -> List[Dict]:
        """Get list of quarterly cycles sorted by most recent first"""
        url = "https://goal.base.vn/extapi/v1/cycle/list"
        data = {'access_token': self.goal_token}

        response = self._make_request(url, data, "fetching cycle list")
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

        response = self._make_request(url, data, "fetching account users")
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

        response = self._make_request(url, data, "fetching goals data")
        data = response.json()

        goals_data = []
        for goal in data.get('goals', []):
            goals_data.append({
                'goal_id': goal.get('id'),
                'goal_name': goal.get('name', 'Unknown Goal'),
                'goal_content': goal.get('content', ''),
                'goal_since': DateUtils.convert_timestamp_to_datetime(goal.get('since')),
                'goal_current_value': goal.get('current_value', 0),
                'goal_user_id': str(goal.get('user_id', '')),
            })

        return pd.DataFrame(goals_data)

    def get_krs_data(self, cycle_path: str) -> pd.DataFrame:
        """Get KRs data from API with pagination"""
        url = "https://goal.base.vn/extapi/v1/cycle/krs"
        all_krs = []
        
        for page in range(1, MAX_PAGES_KRS + 1):
            data = {"access_token": self.goal_token, "path": cycle_path, "page": page}

            response = self._make_request(url, data, f"loading KRs at page {page}")
            response_data = response.json()

            if isinstance(response_data, list) and len(response_data) > 0:
                response_data = response_data[0]

            krs_list = response_data.get("krs", [])
            if not krs_list:
                break

            for kr in krs_list:
                all_krs.append({
                    'kr_id': str(kr.get('id', '')),
                    'kr_name': kr.get('name', 'Unknown KR'),
                    'kr_content': kr.get('content', ''),
                    'kr_since': DateUtils.convert_timestamp_to_datetime(kr.get('since')),
                    'kr_current_value': kr.get('current_value', 0),
                    'kr_user_id': str(kr.get('user_id', '')),
                    'goal_id': kr.get('goal_id'),
                })

        return pd.DataFrame(all_krs)

    def get_all_checkins(self, cycle_path: str) -> List[Dict]:
        """Get all checkins with pagination"""
        url = "https://goal.base.vn/extapi/v1/cycle/checkins"
        all_checkins = []
        
        for page in range(1, MAX_PAGES_CHECKINS + 1):
            data = {"access_token": self.goal_token, "path": cycle_path, "page": page}

            response = self._make_request(url, data, f"loading checkins at page {page}")
            response_data = response.json()

            if isinstance(response_data, list) and len(response_data) > 0:
                response_data = response_data[0]

            checkins = response_data.get('checkins', [])
            if not checkins:
                break

            all_checkins.extend(checkins)

            if len(checkins) < 20:
                break

        return all_checkins


class DataProcessor:
    """Handles data processing and transformations"""
    
    @staticmethod
    def extract_checkin_data(all_checkins: List[Dict]) -> pd.DataFrame:
        """Extract checkin data into DataFrame"""
        checkin_list = []

        for checkin in all_checkins:
            try:
                checkin_data = DataProcessor._process_single_checkin(checkin)
                checkin_list.append(checkin_data)
            except Exception as e:
                st.warning(f"Error processing checkin {checkin.get('id', 'Unknown')}: {e}")
                continue

        return pd.DataFrame(checkin_list)

    @staticmethod
    def _process_single_checkin(checkin: Dict) -> Dict:
        """Process a single checkin record"""
        checkin_id = checkin.get('id', '')
        checkin_name = checkin.get('name', '')
        user_id = str(checkin.get('user_id', ''))
        since_timestamp = checkin.get('since', '')
        since_date = DateUtils.convert_timestamp_to_datetime(since_timestamp) or ''

        # Extract form value
        form_data = checkin.get('form', [])
        form_value = form_data[0].get('value', '') if form_data else ''

        # Extract target info
        obj_export = checkin.get('obj_export', {})
        target_name = obj_export.get('name', '')
        kr_id = str(obj_export.get('id', ''))
        current_value = checkin.get('current_value', 0)

        return {
            'checkin_id': checkin_id,
            'checkin_name': checkin_name,
            'checkin_since': since_date,
            'cong_viec_tiep_theo': form_value,
            'checkin_target_name': target_name,
            'checkin_kr_current_value': current_value,
            'kr_id': kr_id,
            'checkin_user_id': user_id
        }

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
    def calculate_reference_value(reference_date: datetime, df: pd.DataFrame) -> Tuple[float, List[Dict]]:
        """Calculate OKR value as of reference date (works for both Friday and month-end)"""
        try:
            df = df.copy()
            df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')

            unique_krs = df['kr_id'].dropna().unique()
            goal_values_dict = {}
            kr_details = []

            for kr_id in unique_krs:
                kr_data = df[df['kr_id'] == kr_id].copy()
                
                actual_checkins_before_date = kr_data[
                    (kr_data['checkin_since_dt'] <= reference_date) &
                    (kr_data['checkin_name'].notna()) &
                    (kr_data['checkin_name'] != '')
                ]

                goal_name = kr_data.iloc[0]['goal_name'] if len(kr_data) > 0 else f"Unknown_{kr_id}"

                if len(actual_checkins_before_date) > 0:
                    latest_checkin = actual_checkins_before_date.sort_values('checkin_since_dt').iloc[-1]
                    kr_value = pd.to_numeric(latest_checkin['checkin_kr_current_value'], errors='coerce')
                    kr_value = kr_value if not pd.isna(kr_value) else 0

                    if goal_name not in goal_values_dict:
                        goal_values_dict[goal_name] = []
                    goal_values_dict[goal_name].append(kr_value)

                    kr_details.append({
                        'kr_id': kr_id,
                        'goal_name': goal_name,
                        'kr_value': kr_value,
                        'checkin_date': latest_checkin['checkin_since_dt'],
                        'source': f'checkin_before_{reference_date.strftime("%Y%m%d")}'
                    })
                else:
                    goal_key = f"{goal_name}_no_checkin_{kr_id}"
                    goal_values_dict[goal_key] = [0]

                    kr_details.append({
                        'kr_id': kr_id,
                        'goal_name': goal_name,
                        'kr_value': 0,
                        'checkin_date': None,
                        'source': f'no_checkin_before_{reference_date.strftime("%Y%m%d")}'
                    })

            goal_values = [np.mean(kr_values_list) for kr_values_list in goal_values_dict.values()]
            reference_value = np.mean(goal_values) if goal_values else 0
            
            return reference_value, kr_details

        except Exception as e:
            st.error(f"Error calculating reference value: {e}")
            return 0, []

    @staticmethod
    def calculate_kr_shift(row: pd.Series, reference_date: datetime, final_df: pd.DataFrame) -> float:
        """Calculate kr_shift = kr_current_value - reference_date_checkin_value"""
        try:
            kr_current_value = pd.to_numeric(row.get('kr_current_value', 0), errors='coerce')
            kr_current_value = kr_current_value if not pd.isna(kr_current_value) else 0
            
            kr_id = row.get('kr_id', '')
            if not kr_id:
                return kr_current_value
            
            quarter_start = DateUtils.get_quarter_start_date()
            
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
                    (kr_checkins['checkin_since_dt'] <= reference_date)
                ]
                
                if not kr_checkins.empty:
                    latest_checkin = kr_checkins.loc[kr_checkins['checkin_since_dt'].idxmax()]
                    reference_checkin_value = pd.to_numeric(latest_checkin.get('checkin_kr_current_value', 0), errors='coerce')
                    reference_checkin_value = reference_checkin_value if not pd.isna(reference_checkin_value) else 0
                else:
                    reference_checkin_value = 0
            else:
                reference_checkin_value = 0
            
            return kr_current_value - reference_checkin_value
            
        except Exception as e:
            st.warning(f"Error calculating kr_shift: {e}")
            return 0


class EmailReportGenerator:
    """Generate and send email reports for OKR analysis"""
    
    def __init__(self, smtp_server="smtp.gmail.com", smtp_port=587):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def create_visual_html_chart(self, data: Dict, chart_type: str, title: str) -> str:
        """Create HTML-based visual charts"""
        if chart_type == "pie":
            return self._create_pie_chart_html(data, title)
        elif chart_type == "bar":
            return self._create_bar_chart_html(data, title)
        return f"<div class='modern-chart'><h3>{title}</h3><p>Loại biểu đồ không được hỗ trợ</p></div>"

    def _create_pie_chart_html(self, data: Dict, title: str) -> str:
        """Create pie chart HTML"""
        total = sum(data.values())
        if total == 0:
            return f"<div class='chart-fallback'><h4>{title}</h4><p>Không có dữ liệu</p></div>"
        
        colors = ['#27AE60', '#E74C3C', '#3498DB', '#F39C12', '#9B59B6']
        
        html = f"""
        <div class='modern-chart'>
            <h3 style='text-align: center; margin-bottom: 30px; color: #2c3e50; font-size: 20px;'>{title}</h3>
            <div style='display: flex; justify-content: center; align-items: center; gap: 40px; padding: 20px;'>
        """
        
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

    def _create_bar_chart_html(self, data: Dict, title: str) -> str:
        """Create bar chart HTML"""
        if not data:
            return f"<div class='modern-chart'><h3>{title}</h3><p>Không có dữ liệu</p></div>"
        
        max_value = max(abs(v) for v in data.values())
        
        html = f"""
        <div class='modern-chart'>
            <h3 style='text-align: center; margin-bottom: 25px; color: #2c3e50; font-size: 20px;'>{title}</h3>
            <div style='max-height: 500px; overflow-y: auto; padding: 10px;'>
        """
        
        for name, value in list(data.items()):
            width_pct = (abs(value) / max_value * 100) if max_value > 0 else 0
            color, bg_color, icon = self._get_bar_style(value)
            
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

    def _get_bar_style(self, value: float) -> Tuple[str, str, str]:
        """Get style properties for bar chart based on value"""
        if value > 0:
            return '#27AE60', 'rgba(39, 174, 96, 0.1)', '📈'
        elif value < 0:
            return '#E74C3C', 'rgba(231, 76, 60, 0.1)', '📉'
        else:
            return '#F39C12', 'rgba(243, 156, 18, 0.1)', '➡️'

    def _generate_table_html(self, data: List[Dict], headers: List[str], fields: List[str]) -> str:
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

    def _generate_okr_table_html(self, data: List[Dict], period_type: str = "weekly") -> str:
        """Generate HTML table for OKR data (weekly or monthly)"""
        if not data:
            return "<div style='text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; color: #7f8c8d;'><p>📭 Không có dữ liệu</p></div>"
        
        shift_key = 'okr_shift' if period_type == "weekly" else 'okr_shift_monthly'
        value_key = 'last_friday_value' if period_type == "weekly" else 'last_month_value'
        period_label = "(tuần)" if period_type == "weekly" else "(tháng)"
        
        html = f"""
        <table>
            <thead>
                <tr>
                    <th>👤 Nhân viên</th>
                    <th>📊 Dịch chuyển {period_label}</th>
                    <th>🎯 Giá trị hiện tại</th>
                    <th>📅 Giá trị trước đó</th>
                </tr>
            </thead>
            <tbody>
        """
        
        for i, item in enumerate(data):
            shift_value = item.get(shift_key, 0)
            shift_class = "positive" if shift_value > 0 else "negative" if shift_value < 0 else "neutral"
            shift_icon = "📈" if shift_value > 0 else "📉" if shift_value < 0 else "➡️"
            row_class = "even" if i % 2 == 0 else "odd"
            
            html += f"""
            <tr class='{row_class}'>
                <td><strong>{item.get('user_name', 'Unknown')}</strong></td>
                <td class="{shift_class}">{shift_icon} <strong>{shift_value:.2f}</strong></td>
                <td><span style='color: #3498db; font-weight: 600;'>{item.get('current_value', 0):.2f}</span></td>
                <td><span style='color: #7f8c8d;'>{item.get(value_key, 0):.2f}</span></td>
            </tr>
            """
        
        html += "</tbody></table>"
        return html

    def _generate_checkin_overview_table_html(self, overall_checkins_data: List[Dict]) -> str:
        """Generate HTML table for top overall checkin users"""
        if not overall_checkins_data:
            return "<div style='text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; color: #7f8c8d;'><p>📭 Không có dữ liệu</p></div>"
        
        today = datetime.now()
        quarter_start = DateUtils.get_quarter_start_date()
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
            rank_icon = self._get_rank_icon(i)
            frequency = item.get('checkin_frequency_per_week', 0)
            last_week = item.get('last_week_checkins', 0)
            total = item.get('total_checkins', 0)
            
            row_style = self._get_row_style(i)
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
        
        # Add summary row
        if overall_checkins_data:
            html += self._generate_checkin_summary_row(overall_checkins_data)
        
        html += "</tbody></table>"
        return html

    def _get_rank_icon(self, index: int) -> str:
        """Get rank icon for position"""
        rank_icons = {0: "🥇", 1: "🥈", 2: "🥉"}
        return rank_icons.get(index, str(index + 1))

    def _get_row_style(self, index: int) -> str:
        """Get row style for table"""
        if index < 3:
            return "style='background: linear-gradient(135deg, #fff9e6, #fffbf0); font-weight: 600;'"
        elif index % 2 == 0:
            return "style='background: #f8f9fa;'"
        return ""

    def _generate_checkin_summary_row(self, data: List[Dict]) -> str:
        """Generate summary row for checkin table"""
        total_checkins_sum = sum(item.get('total_checkins', 0) for item in data)
        avg_frequency = sum(item.get('checkin_frequency_per_week', 0) for item in data) / len(data)
        active_last_week = len([item for item in data if item.get('last_week_checkins', 0) > 0])
        
        return f"""
        <tr style='background: linear-gradient(135deg, #e8f4f8, #f0f8ff); border-top: 2px solid #3498db; font-weight: bold;'>
            <td colspan="2" style='text-align: center; color: #2c3e50;'>📊 TỔNG KẾT TOP {len(data)}</td>
            <td style='text-align: center; color: #3498db;'>{total_checkins_sum}</td>
            <td style='text-align: center; color: #27AE60;'>{avg_frequency:.2f}</td>
            <td style='text-align: center; color: #e74c3c;'>{active_last_week} người</td>
        </tr>
        """

    def _get_email_styles(self) -> str:
        """Get CSS styles for email"""
        return """
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #2c3e50; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f8f9fa; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; border-radius: 15px; text-align: center; margin-bottom: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.15); }
            .header h1 { margin: 0 0 10px 0; font-size: 28px; font-weight: 700; }
            .header h2 { margin: 0 0 10px 0; font-size: 22px; font-weight: 500; opacity: 0.9; }
            .header p { margin: 0; font-size: 16px; opacity: 0.8; }
            .section { background: white; padding: 30px; margin: 25px 0; border-radius: 15px; box-shadow: 0 5px 20px rgba(0,0,0,0.08); border: 1px solid #e9ecef; }
            .section h2 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; margin-bottom: 25px; font-size: 22px; }
            .metrics { display: flex; justify-content: space-around; margin: 25px 0; flex-wrap: wrap; gap: 15px; }
            .metric { background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%); padding: 25px; border-radius: 12px; text-align: center; box-shadow: 0 4px 15px rgba(0,0,0,0.08); min-width: 140px; flex: 1; border: 1px solid #e9ecef; }
            .metric-value { font-size: 32px; font-weight: 700; color: #3498db; margin-bottom: 5px; }
            .metric-label { font-size: 14px; color: #7f8c8d; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px; }
            table { width: 100%; border-collapse: collapse; margin: 20px 0; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.05); }
            th { padding: 16px; text-align: left; background: linear-gradient(135deg, #3498db, #2980b9); color: white; font-weight: 600; font-size: 14px; text-transform: uppercase; letter-spacing: 0.5px; }
            td { padding: 14px 16px; border-bottom: 1px solid #ecf0f1; font-size: 14px; }
            tr:nth-child(even) { background: #f8f9fa; }
            tr:hover { background: #e8f4f8; transition: background 0.2s ease; }
            .chart-container { text-align: center; margin: 30px 0; }
            .modern-chart { background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%); padding: 30px; border-radius: 15px; box-shadow: 0 8px 25px rgba(0,0,0,0.1); margin: 25px 0; border: 1px solid #e9ecef; }
            .positive { color: #27AE60; font-weight: bold; }
            .negative { color: #E74C3C; font-weight: bold; }
            .neutral { color: #F39C12; font-weight: bold; }
            .footer { text-align: center; margin-top: 40px; padding: 25px; background: linear-gradient(135deg, #2c3e50, #34495e); color: white; border-radius: 15px; }
            .alert { padding: 18px; margin: 20px 0; border-radius: 10px; border-left: 4px solid; }
            .alert-warning { background: linear-gradient(135deg, #fff3cd, #fef8e6); border-left-color: #f39c12; color: #856404; }
            .alert-info { background: linear-gradient(135deg, #d1ecf1, #e8f5f7); border-left-color: #3498db; color: #0c5460; }
            .monthly-indicator { background: linear-gradient(135deg, #e8f5e8, #f0fff0); border: 2px solid #27AE60; border-radius: 10px; padding: 15px; margin: 20px 0; }
        </style>
        """

    def create_email_content(self, analyzer, selected_cycle, members_without_goals, 
                           members_without_checkins, members_with_goals_no_checkins, 
                           okr_shifts, okr_shifts_monthly=None) -> str:
        """Create HTML email content with all analysis data"""
        
        current_date = datetime.now().strftime("%d/%m/%Y")
        total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
        
        # Calculate statistics
        stats = self._calculate_email_stats(total_members, members_without_goals, 
                                          members_without_checkins, okr_shifts, okr_shifts_monthly)
        
        # Get checkin behavior analysis data
        period_checkins, overall_checkins = analyzer.analyze_checkin_behavior()
        
        # Create visual charts
        charts = self._create_email_charts(stats, okr_shifts, okr_shifts_monthly)
        
        # Generate tables
        tables = self._generate_email_tables(members_without_goals, members_without_checkins,
                                           members_with_goals_no_checkins, okr_shifts, 
                                           okr_shifts_monthly, overall_checkins)
        
        # Build HTML content
        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            {self._get_email_styles()}
        </head>
        <body>
            {self._generate_email_header(selected_cycle, current_date)}
            {self._generate_overview_section(stats)}
            {self._generate_monthly_indicator(okr_shifts_monthly)}
            {self._generate_checkin_section(tables['checkins'], stats)}
            {self._generate_okr_sections(charts, tables, stats)}
            {self._generate_detailed_sections(tables)}
            {self._generate_email_footer()}
        </body>
        </html>
        """
        
        return html_content

    def _calculate_email_stats(self, total_members: int, members_without_goals: List, 
                              members_without_checkins: List, okr_shifts: List, 
                              okr_shifts_monthly: Optional[List] = None) -> Dict:
        """Calculate statistics for email report"""
        members_with_goals = total_members - len(members_without_goals)
        members_with_checkins = total_members - len(members_without_checkins)
        
        stats = {
            'total_members': total_members,
            'members_with_goals': members_with_goals,
            'members_with_checkins': members_with_checkins,
            'progress_users': len([u for u in okr_shifts if u['okr_shift'] > 0]) if okr_shifts else 0,
            'stable_users': len([u for u in okr_shifts if u['okr_shift'] == 0]) if okr_shifts else 0,
            'issue_users': len([u for u in okr_shifts if u['okr_shift'] < 0]) if okr_shifts else 0
        }
        
        if okr_shifts_monthly:
            stats.update({
                'progress_users_monthly': len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] > 0]),
                'stable_users_monthly': len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] == 0]),
                'issue_users_monthly': len([u for u in okr_shifts_monthly if u['okr_shift_monthly'] < 0])
            })
        
        return stats

    def _create_email_charts(self, stats: Dict, okr_shifts: List, okr_shifts_monthly: Optional[List]) -> Dict:
        """Create charts for email"""
        charts = {}
        
        # Goal distribution chart
        charts['goal'] = self.create_visual_html_chart(
            {'Có OKR': stats['members_with_goals'], 'Chưa có OKR': stats['total_members'] - stats['members_with_goals']},
            'pie', 'Phân bố trạng thái OKR'
        )
        
        # Weekly OKR shifts chart
        if okr_shifts:
            okr_shifts_data = {u['user_name']: u['okr_shift'] for u in okr_shifts}
            charts['okr_weekly'] = self.create_visual_html_chart(
                okr_shifts_data, 'bar', 'Dịch chuyển OKR tuần (Tất cả NV có goal)'
            )
        
        # Monthly OKR shifts chart
        if okr_shifts_monthly:
            okr_shifts_monthly_data = {u['user_name']: u['okr_shift_monthly'] for u in okr_shifts_monthly}
            charts['okr_monthly'] = self.create_visual_html_chart(
                okr_shifts_monthly_data, 'bar', 'Dịch chuyển OKR tháng (Tất cả NV có goal)'
            )
        
        return charts

    def _generate_email_tables(self, members_without_goals: List, members_without_checkins: List,
                              members_with_goals_no_checkins: List, okr_shifts: List,
                              okr_shifts_monthly: Optional[List], overall_checkins: List) -> Dict:
        """Generate all tables for email"""
        return {
            'goals': self._generate_table_html(members_without_goals, 
                                             ["Tên", "Username", "Chức vụ"], 
                                             ["name", "username", "job"]),
            'checkins': self._generate_table_html(members_without_checkins,
                                                ["Tên", "Username", "Chức vụ", "Có OKR"],
                                                ["name", "username", "job", "has_goal"]),
            'goals_no_checkins': self._generate_table_html(members_with_goals_no_checkins,
                                                         ["Tên", "Username", "Chức vụ"],
                                                         ["name", "username", "job"]),
            'top_performers': self._generate_okr_table_html([u for u in okr_shifts if u['okr_shift'] > 0] if okr_shifts else []),
            'top_performers_monthly': self._generate_okr_table_html([u for u in okr_shifts_monthly if u['okr_shift_monthly'] > 0] if okr_shifts_monthly else [], "monthly"),
            'issue_performers': self._generate_okr_table_html([u for u in okr_shifts if u['okr_shift'] < 0] if okr_shifts else []),
            'issue_performers_monthly': self._generate_okr_table_html([u for u in okr_shifts_monthly if u['okr_shift_monthly'] < 0] if okr_shifts_monthly else [], "monthly"),
            'top_overall': self._generate_checkin_overview_table_html(overall_checkins if overall_checkins else [])
        }

    def _generate_email_header(self, selected_cycle: Dict, current_date: str) -> str:
        """Generate email header"""
        return f"""
        <div class="header">
            <h1>📊 BÁO CÁO TIẾN ĐỘ OKR & CHECKIN</h1>
            <h2>{selected_cycle['name']}</h2>
            <p>Ngày báo cáo: {current_date}</p>
        </div>
        """

    def _generate_overview_section(self, stats: Dict) -> str:
        """Generate overview metrics section"""
        monthly_metric = ""
        if 'progress_users_monthly' in stats:
            monthly_metric = f"""
            <div class="metric">
                <div class="metric-value">{stats['progress_users_monthly']}</div>
                <div class="metric-label">Tiến bộ (tháng)</div>
            </div>
            """
        
        return f"""
        <div class="section">
            <h2>📈 TỔNG QUAN</h2>
            <div class="metrics">
                <div class="metric">
                    <div class="metric-value">{stats['total_members']}</div>
                    <div class="metric-label">Tổng nhân viên</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{stats['members_with_goals']}</div>
                    <div class="metric-label">Có OKR</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{stats['members_with_checkins']}</div>
                    <div class="metric-label">Có Checkin</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{stats['progress_users']}</div>
                    <div class="metric-label">Tiến bộ (tuần)</div>
                </div>
                {monthly_metric}
            </div>
        </div>
        """

    def _generate_monthly_indicator(self, okr_shifts_monthly: Optional[List]) -> str:
        """Generate monthly indicator section"""
        if not okr_shifts_monthly:
            return ""
        
        current_month = datetime.now().month
        month_names = {2: "Tháng 2", 3: "Tháng 3", 5: "Tháng 5", 6: "Tháng 6", 
                      8: "Tháng 8", 9: "Tháng 9", 11: "Tháng 11", 12: "Tháng 12"}
        month_name = month_names.get(current_month, f"Tháng {current_month}")
        
        return f"""
        <div class="monthly-indicator">
            <strong>🗓️ {month_name}:</strong> Báo cáo này bao gồm phân tích dịch chuyển OKR theo tháng (so với cuối tháng trước)
        </div>
        """

    def _generate_checkin_section(self, checkins_table: str, stats: Dict) -> str:
        """Generate checkin analysis section"""
        checkin_pct = (stats['members_with_checkins']/stats['total_members']*100) if stats['total_members'] > 0 else 0
        
        return f"""
        <div class="section">
            <h2>📝 DANH SÁCH NHÂN VIÊN CHƯA CHECKIN</h2>
            <div class="chart-container">
                {checkins_table}
            </div>
            <div class="alert alert-info">
                <strong>Thống kê:</strong> {stats['members_with_checkins']}/{stats['total_members']} nhân viên đã có Checkin ({checkin_pct:.1f}%)
            </div>
        </div>
        """

    def _generate_okr_sections(self, charts: Dict, tables: Dict, stats: Dict) -> str:
        """Generate OKR analysis sections"""
        sections = []
        
        # Weekly OKR section
        if 'okr_weekly' in charts:
            sections.append(f"""
            <div class="section">
                <h2>📊 DỊCH CHUYỂN OKR (TUẦN)</h2>
                <div class="chart-container">
                    {charts['okr_weekly']}
                </div>
                <div class="metrics">
                    <div class="metric">
                        <div class="metric-value positive">{stats['progress_users']}</div>
                        <div class="metric-label">Tiến bộ</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value neutral">{stats['stable_users']}</div>
                        <div class="metric-label">Ổn định</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value negative">{stats['issue_users']}</div>
                        <div class="metric-label">Cần quan tâm</div>
                    </div>
                </div>
            </div>
            """)
        
        # Monthly OKR section
        if 'okr_monthly' in charts:
            sections.append(f"""
            <div class="section">
                <h2>🗓️ DỊCH CHUYỂN OKR (THÁNG)</h2>
                <div class="chart-container">
                    {charts['okr_monthly']}
                </div>
                <div class="metrics">
                    <div class="metric">
                        <div class="metric-value positive">{stats.get('progress_users_monthly', 0)}</div>
                        <div class="metric-label">Tiến bộ</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value neutral">{stats.get('stable_users_monthly', 0)}</div>
                        <div class="metric-label">Ổn định</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value negative">{stats.get('issue_users_monthly', 0)}</div>
                        <div class="metric-label">Cần quan tâm</div>
                    </div>
                </div>
            </div>
            """)
        
        return ''.join(sections)

    def _generate_detailed_sections(self, tables: Dict) -> str:
        """Generate detailed analysis sections"""
        sections = []
        
        # Top overall checkin section
        sections.append(f"""
        <div class="section">
            <h2>🏆 TẤT CẢ NHÂN VIÊN HOẠT ĐỘNG CHECKIN</h2>
            <div class="alert alert-info">
                <strong>Thống kê:</strong> Xếp hạng dựa trên tổng số checkin và tần suất checkin từ đầu quý
            </div>
            {tables['top_overall']}
        </div>
        """)
        
        # Conditional sections
        section_configs = [
            (tables['goals'], "🚫 NHÂN VIÊN CHƯA CÓ OKR", "Cần hành động: Những nhân viên này cần được hỗ trợ thiết lập OKR."),
            (tables['goals_no_checkins'], "⚠️ CÓ OKR NHƯNG CHƯA CHECKIN", "Ưu tiên cao: Đã có mục tiêu nhưng chưa cập nhật tiến độ."),
            (tables['top_performers'], "🏆 TẤT CẢ NHÂN VIÊN TIẾN BỘ (TUẦN)", None),
            (tables['top_performers_monthly'], "🗓️ TẤT CẢ NHÂN VIÊN TIẾN BỘ (THÁNG)", None),
            (tables['issue_performers'], "⚠️ NHÂN VIÊN CẦN HỖ TRỢ (TUẦN)", "Cần quan tâm: OKR của những nhân viên này đang giảm hoặc không tiến triển."),
            (tables['issue_performers_monthly'], "🗓️ NHÂN VIÊN CẦN HỖ TRỢ (THÁNG)", "Cần quan tâm: OKR tháng của những nhân viên này đang giảm hoặc không tiến triển.")
        ]
        
        for table_content, title, alert_msg in section_configs:
            if table_content and not ("📭 Không có dữ liệu" in table_content):
                alert_html = f'<div class="alert alert-warning"><strong>{alert_msg}</strong></div>' if alert_msg else ''
                sections.append(f"""
                <div class="section">
                    <h2>{title}</h2>
                    {alert_html}
                    {table_content}
                </div>
                """)
        
        return ''.join(sections)

    def _generate_email_footer(self) -> str:
        """Generate email footer"""
        return """
        <div class="footer">
            <p><strong>🏢 A Plus Mineral Material Corporation</strong></p>
            <p>📊 Báo cáo được tạo tự động bởi hệ thống OKR Analysis</p>
            <p><em>📧 Đây là email tự động, vui lòng không trả lời email này.</em></p>
        </div>
        """

    def send_email_report(self, email_from: str, password: str, email_to: str, 
                         subject: str, html_content: str, company_name: str = "A Plus Mineral Material Corporation") -> Tuple[bool, str]:
        """Send single email report"""
        try:
            message = MIMEMultipart('related')
            message['From'] = f"OKR System {company_name} <{email_from}>"
            message['To'] = email_to
            message['Subject'] = subject
            
            msg_alternative = MIMEMultipart('alternative')
            message.attach(msg_alternative)
            
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg_alternative.attach(html_part)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(email_from, password)
                server.send_message(message)
            
            return True, "Email sent successfully!"
            
        except smtplib.SMTPAuthenticationError:
            return False, "Lỗi xác thực: Vui lòng kiểm tra lại email và mật khẩu"
        except Exception as e:
            return False, f"Lỗi gửi email: {str(e)}"

    def send_email_report_bulk(self, email_from: str, password: str, recipient_list: List[str], 
                              subject: str, html_content: str, attach_excel: bool = False, 
                              excel_buffer: Optional[BytesIO] = None, excel_filename: str = "okr_report.xlsx") -> Tuple[bool, str, List[str]]:
        """Send email report to multiple recipients with optional Excel attachment"""
        success_count = 0
        failed_count = 0
        errors = []
        
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(email_from, password)
                
                for email_to in recipient_list:
                    try:
                        message = self._create_email_message(email_from, email_to, subject, html_content)
                        
                        # Đính kèm Excel cho TẤT CẢ recipients nếu attach_excel = True
                        if attach_excel and excel_buffer:
                            self._attach_excel_to_message(message, excel_buffer, excel_filename)
                        
                        server.send_message(message)
                        success_count += 1
                        
                    except Exception as e:
                        failed_count += 1
                        errors.append(f"{email_to}: {str(e)}")
            
            return True, f"Successfully sent {success_count} emails, {failed_count} failed", errors
            
        except Exception as e:
            return False, f"Server connection error: {str(e)}", errors

    def _create_email_message(self, email_from: str, email_to: str, subject: str, html_content: str) -> MIMEMultipart:
        """Create email message"""
        message = MIMEMultipart('related')
        message['From'] = f"OKR System A Plus <{email_from}>"
        message['To'] = email_to
        message['Subject'] = subject
        
        msg_alternative = MIMEMultipart('alternative')
        message.attach(msg_alternative)
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg_alternative.attach(html_part)
        
        return message

    def _attach_excel_to_message(self, message: MIMEMultipart, excel_buffer: BytesIO, excel_filename: str):
        """Attach Excel file to email message"""
        excel_part = MIMEBase('application', 'octet-stream')
        excel_part.set_payload(excel_buffer.getvalue())
        encoders.encode_base64(excel_part)
        excel_part.add_header('Content-Disposition', f'attachment; filename= {excel_filename}')
        message.attach(excel_part)


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
    
    def get_total_account_users(self) -> pd.DataFrame:
        """Get all account users (not filtered)"""
        return self.api_client.get_account_users()

    def load_and_process_data(self, progress_callback=None):
        """Main function to load and process all data"""
        try:
            steps = [
                ("Loading filtered members...", 0.05, self._load_filtered_members),
                ("Loading users...", 0.1, self._load_users),
                ("Loading goals...", 0.2, self._load_goals),
                ("Loading KRs...", 0.4, self._load_krs),
                ("Loading checkins...", 0.6, self._load_checkins),
                ("Processing data...", 0.8, self._process_data)
            ]
            
            data_store = {}
            
            for message, progress, step_func in steps:
                if progress_callback:
                    progress_callback(message, progress)
                
                result = step_func(data_store)
                if result is None:
                    return None
            
            if progress_callback:
                progress_callback("Data processing completed!", 1.0)

            return self.final_df

        except Exception as e:
            st.error(f"Error in data processing: {e}")
            return None

    def _load_filtered_members(self, data_store: Dict):
        """Load filtered members data"""
        filtered_members = self.get_filtered_members()
        if filtered_members.empty:
            st.error("Failed to load filtered members data")
            return None
        data_store['filtered_members'] = filtered_members
        return filtered_members

    def _load_users(self, data_store: Dict):
        """Load users data"""
        users_df = self.api_client.get_account_users()
        if users_df.empty:
            st.error("Failed to load users data")
            return None
        data_store['users_df'] = users_df
        data_store['user_id_to_name'] = dict(zip(users_df['id'], users_df['name']))
        return users_df

    def _load_goals(self, data_store: Dict):
        """Load goals data"""
        goals_df = self.api_client.get_goals_data(self.checkin_path)
        if goals_df.empty:
            st.error("Failed to load goals data")
            return None
        data_store['goals_df'] = goals_df
        return goals_df

    def _load_krs(self, data_store: Dict):
        """Load KRs data"""
        krs_df = self.api_client.get_krs_data(self.checkin_path)
        data_store['krs_df'] = krs_df
        return krs_df

    def _load_checkins(self, data_store: Dict):
        """Load checkins data"""
        all_checkins = self.api_client.get_all_checkins(self.checkin_path)
        checkin_df = self.data_processor.extract_checkin_data(all_checkins)
        data_store['checkin_df'] = checkin_df
        return checkin_df

    def _process_data(self, data_store: Dict):
        """Process and join all data"""
        goals_df = data_store['goals_df']
        krs_df = data_store['krs_df']
        checkin_df = data_store['checkin_df']
        user_id_to_name = data_store['user_id_to_name']
        
        # Join all data
        joined_df = goals_df.merge(krs_df, on='goal_id', how='left')
        joined_df['goal_user_name'] = joined_df['goal_user_id'].map(user_id_to_name)
        self.final_df = joined_df.merge(checkin_df, on='kr_id', how='left')
        
        # Clean data
        self.final_df = self.data_processor.clean_final_data(self.final_df)
        return self.final_df

    def analyze_missing_goals_and_checkins(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Analyze members without goals and without checkins"""
        try:
            if self.filtered_members_df is None or self.final_df is None:
                return [], [], []

            users_with_goals = set(self.final_df['goal_user_name'].dropna().unique())
            users_with_checkins = self._get_users_with_checkins()
            all_members = set(self.filtered_members_df['name'].unique())
            
            missing_lists = {'goals': [], 'checkins': [], 'goals_no_checkins': []}
            
            for member_name in all_members:
                member_info = self.filtered_members_df[self.filtered_members_df['name'] == member_name].iloc[0].to_dict()
                member_data = self._create_member_dict(member_info)
                
                has_goal = member_name in users_with_goals
                has_checkin = member_name in users_with_checkins
                
                if not has_goal:
                    missing_lists['goals'].append(member_data)
                
                if not has_checkin:
                    member_data_with_goal = member_data.copy()
                    member_data_with_goal['has_goal'] = has_goal
                    missing_lists['checkins'].append(member_data_with_goal)
                
                if has_goal and not has_checkin:
                    missing_lists['goals_no_checkins'].append(member_data)
            
            return missing_lists['goals'], missing_lists['checkins'], missing_lists['goals_no_checkins']
            
        except Exception as e:
            st.error(f"Error analyzing missing goals and checkins: {e}")
            return [], [], []

    def _get_users_with_checkins(self) -> set:
        """Get set of users who have made checkins"""
        users_with_checkins = set()
        if 'checkin_user_id' in self.final_df.columns:
            user_id_to_name = dict(zip(self.filtered_members_df['id'], self.filtered_members_df['name']))
            checkin_user_ids = self.final_df['checkin_user_id'].dropna().unique()
            users_with_checkins = {
                user_id_to_name.get(uid, uid) 
                for uid in checkin_user_ids 
                if uid in user_id_to_name
            }
        return users_with_checkins

    def _create_member_dict(self, member_info: Dict) -> Dict:
        """Create standardized member dictionary"""
        return {
            'name': member_info.get('name', ''),
            'username': member_info.get('username', ''),
            'job': member_info.get('job', ''),
            'email': member_info.get('email', ''),
            'id': member_info.get('id', '')
        }

    def calculate_okr_shifts_by_user(self) -> List[Dict]:
        """Calculate OKR shifts for each user (weekly)"""
        return self._calculate_okr_shifts_by_period("weekly")

    def calculate_okr_shifts_by_user_monthly(self) -> List[Dict]:
        """Calculate monthly OKR shifts for each user"""
        if not DateUtils.should_calculate_monthly_shift():
            return []
        return self._calculate_okr_shifts_by_period("monthly")

    def _calculate_okr_shifts_by_period(self, period: str) -> List[Dict]:
        """Calculate OKR shifts for specified period (weekly or monthly)"""
        try:
            users = self.final_df['goal_user_name'].dropna().unique()
            user_okr_shifts = []
            
            reference_date = DateUtils.get_last_friday_date() if period == "weekly" else DateUtils.get_last_month_end_date()
            
            for user in users:
                user_df = self.final_df[self.final_df['goal_user_name'] == user].copy()
                shift_data = self._calculate_user_shift_data(user_df, reference_date, period)
                user_okr_shifts.append(shift_data)
            
            shift_key = 'okr_shift' if period == "weekly" else 'okr_shift_monthly'
            return sorted(user_okr_shifts, key=lambda x: x[shift_key], reverse=True)
            
        except Exception as e:
            st.error(f"Error calculating {period} OKR shifts: {e}")
            return []

    def _calculate_user_shift_data(self, user_df: pd.DataFrame, reference_date: datetime, period: str) -> Dict:
        """Calculate shift data for a single user"""
        user_name = user_df['goal_user_name'].iloc[0] if not user_df.empty else 'Unknown'
        
        if period == "weekly":
            return self._calculate_weekly_shift_data(user_df, user_name, reference_date)
        else:
            return self._calculate_monthly_shift_data(user_df, user_name, reference_date)

    def _calculate_weekly_shift_data(self, user_df: pd.DataFrame, user_name: str, reference_friday: datetime) -> Dict:
        """Calculate weekly shift data for user"""
        final_okr_goal_shift = self._calculate_final_okr_goal_shift(user_df, reference_friday, "weekly")
        current_value = self.okr_calculator.calculate_current_value(user_df)
        reference_value, kr_details = self.okr_calculator.calculate_reference_value(reference_friday, user_df)
        
        # Áp dụng logic mới theo yêu cầu:
        # 1. Nếu giá trị thứ 6 tuần trước > giá trị hiện tại thì giá trị thứ 6 = giá trị hiện tại - dịch chuyển tuần
        # 2. Nếu giá trị thứ 6 tuần trước < giá trị hiện tại và (giá trị hiện tại - giá trị thứ 6 tuần trước) != dịch chuyển
        #    thì dịch chuyển tuần = giá trị hiện tại - giá trị thứ 6 tuần trước
        
        adjusted_reference_value = reference_value
        adjusted_okr_shift = final_okr_goal_shift
        reference_adjustment_applied = False
        shift_adjustment_applied = False
        
        # Quy tắc 1: Nếu reference_value > current_value
        if reference_value > current_value:
            adjusted_reference_value = current_value - final_okr_goal_shift
            reference_adjustment_applied = True
        
        # Quy tắc 2: Nếu reference_value < current_value VÀ (current_value - reference_value) != shift
        elif reference_value < current_value and (current_value - reference_value) != final_okr_goal_shift:
            adjusted_okr_shift = current_value - reference_value
            shift_adjustment_applied = True
        
        legacy_okr_shift = current_value - reference_value

        return {
            'user_name': user_name,
            'okr_shift': adjusted_okr_shift,
            'original_shift': final_okr_goal_shift,
            'current_value': current_value,
            'last_friday_value': adjusted_reference_value,
            'original_last_friday_value': reference_value,
            'legacy_okr_shift': legacy_okr_shift,
            'adjustment_applied': shift_adjustment_applied,
            'reference_adjustment_applied': reference_adjustment_applied,
            'kr_details_count': len(kr_details),
            'reference_friday': reference_friday.strftime('%d/%m/%Y')
        }

    def _calculate_monthly_shift_data(self, user_df: pd.DataFrame, user_name: str, reference_month_end: datetime) -> Dict:
        """Calculate monthly shift data for user"""
        final_okr_goal_shift_monthly = self._calculate_final_okr_goal_shift(user_df, reference_month_end, "monthly")
        current_value = self.okr_calculator.calculate_current_value(user_df)
        reference_value, kr_details = self.okr_calculator.calculate_reference_value(reference_month_end, user_df)
        
        # Áp dụng logic mới theo yêu cầu:
        # 1. Nếu giá trị cuối tháng trước > giá trị hiện tại thì giá trị cuối tháng = giá trị hiện tại - dịch chuyển tháng
        # 2. Nếu giá trị cuối tháng trước < giá trị hiện tại và (giá trị hiện tại - giá trị cuối tháng trước) != dịch chuyển
        #    thì dịch chuyển tháng = giá trị hiện tại - giá trị cuối tháng trước
        
        adjusted_reference_value = reference_value
        adjusted_okr_shift = final_okr_goal_shift_monthly
        reference_adjustment_applied = False
        shift_adjustment_applied = False
        
        # Quy tắc 1: Nếu reference_value > current_value
        if reference_value > current_value:
            adjusted_reference_value = current_value - final_okr_goal_shift_monthly
            reference_adjustment_applied = True
        
        # Quy tắc 2: Nếu reference_value < current_value VÀ (current_value - reference_value) != shift
        elif reference_value < current_value and (current_value - reference_value) != final_okr_goal_shift_monthly:
            adjusted_okr_shift = current_value - reference_value
            shift_adjustment_applied = True
        
        legacy_okr_shift = current_value - reference_value

        return {
            'user_name': user_name,
            'okr_shift_monthly': adjusted_okr_shift,
            'original_shift_monthly': final_okr_goal_shift_monthly,
            'current_value': current_value,
            'last_month_value': adjusted_reference_value,
            'original_last_month_value': reference_value,
            'legacy_okr_shift_monthly': legacy_okr_shift,
            'adjustment_applied': shift_adjustment_applied,
            'reference_adjustment_applied': reference_adjustment_applied,
            'kr_details_count': len(kr_details),
            'reference_month_end': reference_month_end.strftime('%d/%m/%Y')
        }

    def _calculate_final_okr_goal_shift(self, user_df: pd.DataFrame, reference_date: datetime, period: str) -> float:
        """Calculate final OKR goal shift for specified period"""
        try:
            unique_combinations = {}
            
            for _, row in user_df.iterrows():
                goal_name = row.get('goal_name', '')
                kr_name = row.get('kr_name', '')
                
                if not goal_name or not kr_name:
                    continue
                
                combo_key = f"{goal_name}|{kr_name}"
                kr_shift = self.okr_calculator.calculate_kr_shift(row, reference_date, self.final_df)
                
                if combo_key not in unique_combinations:
                    unique_combinations[combo_key] = []
                unique_combinations[combo_key].append(kr_shift)
            
            final_shifts = [
                sum(kr_shifts) / len(kr_shifts) 
                for kr_shifts in unique_combinations.values() 
                if kr_shifts
            ]
            
            return sum(final_shifts) / len(final_shifts) if final_shifts else 0
            
        except Exception as e:
            st.error(f"Error calculating final_okr_goal_shift: {e}")
            return 0

    def analyze_checkin_behavior(self) -> Tuple[List[Dict], List[Dict]]:
        """Analyze checkin behavior for both period and overall"""
        try:
            last_friday = DateUtils.get_last_friday_date()
            quarter_start = DateUtils.get_quarter_start_date()

            df = self.final_df.copy()
            df['checkin_since_dt'] = pd.to_datetime(df['checkin_since'], errors='coerce')

            # Period analysis (quarter start to last Friday)
            mask_period = (df['checkin_since_dt'] >= quarter_start) & (df['checkin_since_dt'] <= last_friday)
            period_df = df[mask_period].copy()

            # Overall analysis (all time)
            all_time_df = df[df['checkin_id'].notna()].copy()

            all_users = df['goal_user_name'].dropna().unique()

            period_checkins = self._analyze_period_checkins(period_df, all_users)
            overall_checkins = self._analyze_overall_checkins(all_time_df, all_users)

            return period_checkins, overall_checkins

        except Exception as e:
            st.error(f"Error analyzing checkin behavior: {e}")
            return [], []

    def _analyze_period_checkins(self, period_df: pd.DataFrame, all_users: List[str]) -> List[Dict]:
        """Analyze checkins in the reference period"""
        period_checkins = []

        for user in all_users:
            try:
                user_period_data = period_df[period_df['goal_user_name'] == user]
                
                user_period_checkins = user_period_data[
                    (user_period_data['checkin_name'].notna()) &
                    (user_period_data['checkin_name'] != '')
                ]['checkin_id'].nunique()

                user_krs_in_period = user_period_data['kr_id'].nunique()
                checkin_rate = (user_period_checkins / user_krs_in_period * 100) if user_krs_in_period > 0 else 0

                user_checkin_dates = user_period_data[
                    (user_period_data['checkin_name'].notna()) &
                    (user_period_data['checkin_name'] != '')
                ]['checkin_since_dt'].dropna()

                first_checkin = user_checkin_dates.min() if len(user_checkin_dates) > 0 else None
                last_checkin = user_checkin_dates.max() if len(user_checkin_dates) > 0 else None
                days_between = (last_checkin - first_checkin).days if first_checkin and last_checkin else 0

                period_checkins.append({
                    'user_name': user,
                    'checkin_count_period': user_period_checkins,
                    'kr_count_period': user_krs_in_period,
                    'checkin_rate_period': checkin_rate,
                    'first_checkin_period': first_checkin,
                    'last_checkin_period': last_checkin,
                    'days_between_checkins': days_between
                })
            except Exception as e:
                st.warning(f"Error analyzing period checkins for {user}: {e}")
                continue

        return sorted(period_checkins, key=lambda x: x['checkin_count_period'], reverse=True)

    def _analyze_overall_checkins(self, all_time_df: pd.DataFrame, all_users: List[str]) -> List[Dict]:
        """Analyze overall checkin behavior"""
        overall_checkins = []
        
        today = datetime.now()
        quarter_start = DateUtils.get_quarter_start_date()
        weeks_in_quarter = max((today - quarter_start).days / 7, 1)
        
        # Calculate last week boundaries
        days_since_monday = today.weekday()
        monday_this_week = today - timedelta(days=days_since_monday)
        monday_last_week = monday_this_week - timedelta(days=7)
        sunday_last_week = monday_last_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
        for user in all_users:
            try:
                user_data = all_time_df[all_time_df['goal_user_name'] == user]
                
                user_total_checkins = user_data['checkin_id'].nunique()
                user_total_krs = self.final_df[self.final_df['goal_user_name'] == user]['kr_id'].nunique()
                checkin_rate = (user_total_checkins / user_total_krs * 100) if user_total_krs > 0 else 0
    
                user_checkins_dates = user_data['checkin_since_dt'].dropna()
                first_checkin = user_checkins_dates.min() if len(user_checkins_dates) > 0 else None
                last_checkin = user_checkins_dates.max() if len(user_checkins_dates) > 0 else None
                days_active = (last_checkin - first_checkin).days if first_checkin and last_checkin else 0
    
                checkin_frequency = user_total_checkins / weeks_in_quarter
                
                # Count last week checkins
                last_week_checkins = user_checkins_dates[
                    (user_checkins_dates >= monday_last_week) & 
                    (user_checkins_dates <= sunday_last_week)
                ] if len(user_checkins_dates) > 0 else []
                
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

# ==================== UTILITY FUNCTIONS ====================

def create_user_manager_with_monthly_calculation(analyzer):
    """Create UserManager integrated with monthly OKR calculation from OKRAnalysisSystem"""
    
    # Chỉ tạo account_df cho users thực sự có OKR data để đồng nhất với OKR shifts analysis
    if analyzer.final_df is not None and not analyzer.final_df.empty:
        # Lấy tất cả unique users có OKR data từ final_df
        users_with_okr_data = set(analyzer.final_df['goal_user_name'].dropna().unique())
        
        # Tạo account_df chỉ từ users có OKR data
        if analyzer.filtered_members_df is not None and not analyzer.filtered_members_df.empty:
            # Lọc filtered_members_df để chỉ lấy users có OKR data
            account_df = analyzer.filtered_members_df[
                analyzer.filtered_members_df['name'].isin(users_with_okr_data)
            ].copy()
            
            # Nếu có users có OKR data nhưng không có trong filtered_members_df, tạo record cơ bản
            existing_names = set(account_df['name'].dropna().unique()) if not account_df.empty and 'name' in account_df.columns else set()
            missing_users = users_with_okr_data - existing_names
            
            if missing_users:
                missing_records = []
                for user_name in missing_users:
                    # Lấy thông tin từ final_df
                    user_data = analyzer.final_df[analyzer.final_df['goal_user_name'] == user_name].iloc[0]
                    missing_records.append({
                        'name': user_name,
                        'username': user_data.get('goal_user_username', user_name.lower()),
                        'email': f"{user_data.get('goal_user_username', user_name.lower())}@company.com",
                        'job': 'N/A',
                        'id': f"okr_{hash(user_name) % 10000}"
                    })
                
                # Thêm missing users vào account_df
                if missing_records:
                    missing_df = pd.DataFrame(missing_records)
                    account_df = pd.concat([account_df, missing_df], ignore_index=True)
        else:
            # Nếu không có filtered_members_df, tạo từ final_df
            user_records = []
            for user_name in users_with_okr_data:
                user_data = analyzer.final_df[analyzer.final_df['goal_user_name'] == user_name].iloc[0]
                user_records.append({
                    'name': user_name,
                    'username': user_data.get('goal_user_username', user_name.lower()),
                    'email': f"{user_data.get('goal_user_username', user_name.lower())}@company.com",
                    'job': 'N/A',
                    'id': f"okr_{hash(user_name) % 10000}"
                })
            account_df = pd.DataFrame(user_records)
        
        krs_df = _extract_krs_data_for_user_manager(analyzer)
        checkin_df = _extract_checkin_data_for_user_manager(analyzer)
    else:
        account_df = analyzer.filtered_members_df if analyzer.filtered_members_df is not None else pd.DataFrame()
        krs_df, checkin_df = pd.DataFrame(), pd.DataFrame()

    return UserManager(account_df, krs_df, checkin_df, analyzer.final_df, analyzer.final_df)

def _extract_krs_data_for_user_manager(analyzer) -> pd.DataFrame:
    """Extract KRs data for UserManager from final_df"""
    krs_data = []
    for _, row in analyzer.final_df.iterrows():
        if pd.notna(row.get('kr_id')):
            user_name = row.get('goal_user_name', '')
            user_id = _get_user_id_from_name(analyzer, user_name)
            
            if user_id:
                krs_data.append({
                    'user_id': user_id,
                    'kr_id': row.get('kr_id'),
                    'current_value': row.get('kr_current_value', 0)
                })
    
    return pd.DataFrame(krs_data)

def _extract_checkin_data_for_user_manager(analyzer) -> pd.DataFrame:
    """Extract checkin data for UserManager from final_df"""
    checkin_data = []
    for _, row in analyzer.final_df.iterrows():
        if pd.notna(row.get('checkin_id')):
            user_name = row.get('goal_user_name', '')
            user_id = _get_user_id_from_name(analyzer, user_name)
            
            if user_id and pd.notna(row.get('checkin_since')):
                try:
                    checkin_datetime = pd.to_datetime(row['checkin_since'])
                    timestamp = checkin_datetime.timestamp()
                    
                    checkin_data.append({
                        'user_id': user_id,
                        'day': timestamp,
                        'checkin_id': row.get('checkin_id')
                    })
                except:
                    continue
    
    return pd.DataFrame(checkin_data)

def _get_user_id_from_name(analyzer, user_name: str) -> Optional[str]:
    """Get user_id from user_name using filtered_members_df"""
    if not user_name:
        return None
    
    for uid, name in analyzer.filtered_members_df.set_index('id')['name'].items():
        if name == user_name:
            return uid
    return None

def export_to_excel(users: List[User], filename: str = "output1.xlsx") -> openpyxl.Workbook:
    """Export user OKR data to Excel with improved formatting"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "OKRs"

    # Define styles
    styles = {
        'header_fill': PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid"),
        'header_font': Font(bold=True, color="FFFFFF"),
        'category_fill': PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid"),
        'category_font': Font(bold=True),
        'thin_border': Border(left=Side(style="thin"), right=Side(style="thin"), 
                             top=Side(style="thin"), bottom=Side(style="thin"))
    }

    # Create title and headers
    _create_excel_title_and_headers(ws, users, styles)
    
    # Create criteria rows
    _create_excel_criteria_rows(ws, styles)
    
    # Fill user data
    _fill_excel_user_data(ws, users, styles)
    
    # Apply formatting
    _apply_excel_formatting(ws, users)
    
    return wb

def _create_excel_title_and_headers(ws, users: List[User], styles: Dict):
    """Create title and headers for Excel file"""
    total_columns = 3 + len(users)
    last_col_letter = get_column_letter(total_columns)
    
    # Title
    ws.merge_cells(f"A1:{last_col_letter}1")
    title_cell = ws["A1"]
    title_cell.value = "ĐÁNH GIÁ OKRs THÁNG"
    title_cell.font = Font(size=14, bold=True)
    title_cell.alignment = Alignment(horizontal="center", vertical="center")

    # Headers
    fixed_headers = ["TT", "Nội dung", "Tự chấm điểm"]
    user_headers = [user.name for user in users]
    headers = fixed_headers + user_headers
    
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=2, column=col_idx, value=header)
        cell.fill = styles['header_fill']
        cell.font = styles['header_font']
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = styles['thin_border']

def _create_excel_criteria_rows(ws, styles: Dict):
    """Create criteria rows in Excel"""
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
            cell.border = styles['thin_border']
            
            if col_idx == 1 and isinstance(value, int):
                cell.fill = styles['category_fill']
                cell.font = styles['category_font']

def _fill_excel_user_data(ws, users: List[User], styles: Dict):
    """Fill user data in Excel"""
    for idx, user in enumerate(users, start=1):
        col_idx = 3 + idx
        
        # Basic scores
        ws.cell(row=3, column=col_idx, value=1 if user.co_OKR == 1 else 0)
        ws.cell(row=4, column=col_idx, value=0.5 if user.checkin == 1 else 0)
        ws.cell(row=5, column=col_idx, value=0.5)

        # Movement percentage and score
        movement = user.dich_chuyen_OKR
        ws.cell(row=6, column=col_idx, value=f"{movement}%")

        # Determine movement score and row
        movement_score, movement_row = _get_movement_score_and_row(movement)
        ws.cell(row=movement_row, column=col_idx, value=movement_score)

        # Total score
        ws.cell(row=14, column=col_idx, value=user.score)

        # Apply formatting
        for r in range(3, 15):
            cell = ws.cell(row=r, column=col_idx)
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = styles['thin_border']

def _get_movement_score_and_row(movement: float) -> Tuple[float, int]:
    """Get movement score and corresponding row number"""
    movement_ranges = [
        (10, 0.15, 7), (26, 0.25, 8), (31, 0.5, 9), (51, 0.75, 10),
        (81, 1.25, 11), (100, 1.5, 12), (float('inf'), 2.5, 13)
    ]
    
    for threshold, score, row in movement_ranges:
        if movement < threshold:
            return score, row
    
    return 2.5, 13  # Default case

def _apply_excel_formatting(ws, users: List[User]):
    """Apply final formatting to Excel worksheet"""
    # Set column widths
    column_widths = {1: 5, 2: 70}  # TT and Nội dung columns
    for col_idx in range(1, 4 + len(users)):
        col_letter = get_column_letter(col_idx)
        width = column_widths.get(col_idx, 15)
        ws.column_dimensions[col_letter].width = width

    # Freeze panes
    ws.freeze_panes = ws["D3"]

def get_email_list(analyzer) -> List[str]:
    """Get email list from filtered members"""
    if analyzer.filtered_members_df is not None:
        email_list = analyzer.filtered_members_df['email'].dropna().tolist()
        return [email for email in email_list if email.strip()]
    return []

def get_default_recipients() -> List[str]:
    """Get default special recipients"""
    return ["xnk3@apluscorp.vn"]


# ==================== STREAMLIT UI FUNCTIONS ====================

def show_user_score_analysis(analyzer):
    """Show user score analysis using integrated monthly calculation"""
    st.subheader("🏆 User Score Analysis (Integrated Monthly Calculation)")
    
    try:
        # Đảm bảo đồng nhất với OKR shifts analysis
        if analyzer.final_df is not None and not analyzer.final_df.empty:
            total_okr_users = len(set(analyzer.final_df['goal_user_name'].dropna().unique()))
            st.info(f"📊 Analyzing {total_okr_users} users with OKR data (same as OKR shifts analysis)")
        
        user_manager = create_user_manager_with_monthly_calculation(analyzer)
        user_manager.update_checkins()
        user_manager.update_okr_movement()
        user_manager.calculate_scores()
        
        users = user_manager.get_users()
        scores_df = _create_user_scores_dataframe(users)
        
        if not scores_df.empty:
            # Validation - số lượng phải khớp với OKR analysis
            score_count = len(scores_df)
            if analyzer.final_df is not None and not analyzer.final_df.empty:
                okr_count = len(set(analyzer.final_df['goal_user_name'].dropna().unique()))
                if score_count != okr_count:
                    st.warning(f"⚠️ Data mismatch detected: Score analysis has {score_count} users, OKR analysis has {okr_count} users")
                else:
                    st.success(f"✅ Data consistency confirmed: {score_count} users in both analyses")
            
            _display_score_metrics(scores_df)
            _display_score_distribution(scores_df)
            _display_score_tables(scores_df)
            _display_score_export_options(scores_df, users)
            return scores_df
        else:
            st.warning("No user score data available")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error in user score analysis: {e}")
        return pd.DataFrame()

def _create_user_scores_dataframe(users: List[User]) -> pd.DataFrame:
    """Create dataframe from users for score analysis"""
    user_data = []
    for user in users:
        user_data.append({
            'Name': user.name,
            'Has OKR': 'Yes' if user.co_OKR == 1 else 'No',
            'Check-in': 'Yes' if user.checkin == 1 else 'No',
            'OKR Movement': user.dich_chuyen_OKR,
            'Score': user.score
        })
    return pd.DataFrame(user_data)

def _display_score_metrics(scores_df: pd.DataFrame):
    """Display score summary metrics"""
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

def _display_score_distribution(scores_df: pd.DataFrame):
    """Display score distribution chart"""
    fig_scores = px.histogram(
        scores_df, 
        x='Score',
        nbins=20,
        title="Distribution of User Scores (with Monthly OKR Calculation)",
        labels={'Score': 'User Score', 'count': 'Number of Users'}
    )
    st.plotly_chart(fig_scores, use_container_width=True)

def _display_score_tables(scores_df: pd.DataFrame):
    """Display score tables"""
    # All performers sorted by score
    st.subheader("📊 Tất cả nhân viên có goal (sắp xếp theo điểm)")
    all_performers = scores_df.sort_values('Score', ascending=False)
    st.dataframe(all_performers, use_container_width=True, hide_index=True)
    
    # Users needing support
    low_performers_df = scores_df[scores_df['Score'] < 2.0]
    if not low_performers_df.empty:
        st.subheader("⚠️ Users Needing Support")
        st.dataframe(low_performers_df, use_container_width=True, hide_index=True)

def _display_score_export_options(scores_df: pd.DataFrame, users: List[User]):
    """Display export options for scores"""
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Export User Scores"):
            csv = scores_df.to_csv(index=False)
            st.download_button(
                label="Download User Scores CSV",
                data=csv,
                file_name=f"user_scores_monthly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col2:
        if st.button("📋 Export to Excel Format"):
            # Validation cho Streamlit Excel export
            st.info(f"📊 Preparing Excel export for {len(users)} users")
            
            wb = export_to_excel(users)
            excel_buffer = BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            st.download_button(
                label="Download Excel Report",
                data=excel_buffer.getvalue(),
                file_name=f"okr_report_monthly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success(f"✅ Excel file ready with {len(users)} users")

def show_data_summary(df: pd.DataFrame, analyzer):
    """Show data summary statistics"""
    st.subheader("📈 Data Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    metrics = [
        ("Total Goals", df['goal_id'].nunique()),
        ("Total KRs", df['kr_id'].nunique()),
        ("Total Checkins", df['checkin_id'].nunique()),
        ("Total Users", df['goal_user_name'].nunique()),
        ("Filtered Members", len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0)
    ]
    
    for col, (label, value) in zip([col1, col2, col3, col4, col5], metrics):
        with col:
            st.metric(label, value)

def show_missing_analysis_section(analyzer):
    """Show missing goals and checkins analysis"""
    members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
    
    _display_missing_summary_metrics(analyzer, members_without_goals, members_without_checkins, members_with_goals_no_checkins)
    _display_missing_visualizations(analyzer, members_without_goals, members_without_checkins, members_with_goals_no_checkins)

def _display_missing_summary_metrics(analyzer, members_without_goals: List, members_without_checkins: List, members_with_goals_no_checkins: List):
    """Display summary metrics for missing analysis"""
    col1, col2, col3, col4 = st.columns(4)
    
    total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
    
    metrics = [
        ("Total Filtered Members", total_members, None),
        ("Members Without Goals", len(members_without_goals), f"{len(members_without_goals)/total_members*100:.1f}%" if total_members > 0 else "0%"),
        ("Members Without Checkins", len(members_without_checkins), f"{len(members_without_checkins)/total_members*100:.1f}%" if total_members > 0 else "0%"),
        ("Has Goals but No Checkins", len(members_with_goals_no_checkins), f"{len(members_with_goals_no_checkins)/total_members*100:.1f}%" if total_members > 0 else "0%")
    ]
    
    for col, (label, value, delta) in zip([col1, col2, col3, col4], metrics):
        with col:
            st.metric(label, value, delta=delta)

def _display_missing_visualizations(analyzer, members_without_goals: List, members_without_checkins: List, members_with_goals_no_checkins: List):
    """Display visualizations for missing analysis"""
    st.subheader("📊 Missing Analysis Visualization")
    
    col1, col2 = st.columns(2)
    
    with col1:
        _display_goals_pie_chart_and_table(analyzer, members_without_goals)
    
    with col2:
        _display_checkins_pie_chart_and_table(members_with_goals_no_checkins)

def _display_goals_pie_chart_and_table(analyzer, members_without_goals: List):
    """Display goals pie chart and table"""
    total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
    members_with_goals = total_members - len(members_without_goals)
    
    goal_data = pd.DataFrame({
        'Status': ['Have Goals', 'No Goals'],
        'Count': [members_with_goals, len(members_without_goals)]
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
        st.dataframe(no_goals_df[['name', 'username', 'job', 'email']], use_container_width=True, height=300)
        
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

def _display_checkins_pie_chart_and_table(members_with_goals_no_checkins: List):
    """Display checkins analysis"""
    if members_with_goals_no_checkins:
        st.subheader("⚠️ Members with Goals but No Checkins")
        st.warning("These members have set up goals but haven't made any checkins yet. They may need guidance or reminders.")
        
        goals_no_checkins_df = pd.DataFrame(members_with_goals_no_checkins)
        st.dataframe(goals_no_checkins_df[['name', 'username', 'job', 'email']], use_container_width=True, height=300)
        
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

def _show_okr_user_selection(analyzer) -> List[str]:
    """Show interface to select specific OKR users"""
    try:
        # Get users with OKRs and their details
        if analyzer.final_df is None or analyzer.filtered_members_df is None:
            st.warning("Data not loaded yet. Please run analysis first.")
            return []
        
        users_with_goals = set(analyzer.final_df['goal_user_name'].dropna().unique())
        
        # Create options list with name and email
        okr_user_options = []
        okr_user_emails = {}
        
        for _, member in analyzer.filtered_members_df.iterrows():
            if member['name'] in users_with_goals and pd.notna(member['email']) and member['email'].strip():
                display_name = f"{member['name']} ({member['email']})"
                okr_user_options.append(display_name)
                okr_user_emails[display_name] = member['email'].strip()
        
        if not okr_user_options:
            st.warning("No OKR users with valid email addresses found")
            return []
        
        # Show multiselect
        st.write(f"Select recipients from {len(okr_user_options)} OKR users:")
        selected_users = st.multiselect(
            "Choose specific OKR users:",
            options=okr_user_options,
            default=okr_user_options,  # Select all by default
            key="okr_user_selection"
        )
        
        # Return selected emails
        return [okr_user_emails[user] for user in selected_users]
        
    except Exception as e:
        st.error(f"Error showing OKR user selection: {e}")
        return []

def show_okr_analysis(okr_shifts: List[Dict], reference_date: datetime, period: str = "weekly"):
    """Show OKR shift analysis with reference date"""
    period_label = "tuần" if period == "weekly" else "tháng"
    shift_key = 'okr_shift' if period == "weekly" else 'okr_shift_monthly'
    last_value_key = 'last_friday_value' if period == "weekly" else 'last_month_value'
    
    reference_label = f"thứ 6 {period_label} trước" if period == "weekly" else f"cuối {period_label} trước"
    
    # Display user count and reference information
    st.info(f"👥 **Tổng số nhân viên:** {len(okr_shifts)} users có OKR data")
    st.info(f"📅 **Ngày tham chiếu:** {reference_label.title()} ({reference_date.strftime('%d/%m/%Y')})")
    st.info(f"📊 **Logic tính toán:** So sánh giá trị hiện tại với giá trị tại {reference_label}")
    
    # Summary metrics
    _display_okr_summary_metrics(okr_shifts, shift_key)
    
    # OKR shift chart
    _display_okr_shift_chart(okr_shifts, shift_key, period_label, reference_label, reference_date)
    
    # Tables
    _display_okr_tables(okr_shifts, shift_key, last_value_key, period_label, reference_label)

def _display_okr_summary_metrics(okr_shifts: List[Dict], shift_key: str):
    """Display OKR summary metrics"""
    col1, col2, col3, col4 = st.columns(4)
    
    progress_users = len([u for u in okr_shifts if u[shift_key] > 0])
    stable_users = len([u for u in okr_shifts if u[shift_key] == 0])
    issue_users = len([u for u in okr_shifts if u[shift_key] < 0])
    avg_shift = np.mean([u[shift_key] for u in okr_shifts])
    
    total_users = len(okr_shifts)
    
    with col1:
        st.metric("Tiến bộ", progress_users, delta=f"{progress_users/total_users*100:.1f}%" if total_users > 0 else "0%")
    
    with col2:
        st.metric("Ổn định", stable_users, delta=f"{stable_users/total_users*100:.1f}%" if total_users > 0 else "0%")
    
    with col3:
        st.metric("Cần hỗ trợ", issue_users, delta=f"{issue_users/total_users*100:.1f}%" if total_users > 0 else "0%")
    
    with col4:
        st.metric("Dịch chuyển TB", f"{avg_shift:.2f}")

def _display_okr_shift_chart(okr_shifts: List[Dict], shift_key: str, period_label: str, reference_label: str, reference_date: datetime):
    """Display OKR shift chart"""
    okr_df = pd.DataFrame(okr_shifts)
    
    fig = px.bar(
        okr_df, 
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

def _display_okr_tables(okr_shifts: List[Dict], shift_key: str, last_value_key: str, period_label: str, reference_label: str):
    """Display OKR performance tables"""
    okr_df = pd.DataFrame(okr_shifts)
    
    # All performers with positive shift
    st.subheader(f"📈 Tất cả nhân viên tiến bộ ({period_label})")
    positive_performers = okr_df[okr_df[shift_key] > 0]
    if not positive_performers.empty:
        display_cols = ['user_name', shift_key, 'current_value', last_value_key]
        display_df = positive_performers[display_cols].round(2)
        display_df.columns = ['Nhân viên', f'Dịch chuyển ({period_label})', 'Giá trị hiện tại', f'Giá trị {reference_label}']
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info(f"Không có nhân viên nào có dịch chuyển OKR {period_label} dương")
    
    # Issues
    issue_users = okr_df[okr_df[shift_key] < 0]
    if not issue_users.empty:
        st.subheader(f"⚠️ Nhân viên cần hỗ trợ ({period_label})")
        display_cols = ['user_name', shift_key, 'current_value', last_value_key]
        display_df = issue_users[display_cols].round(2)
        display_df.columns = ['Nhân viên', f'Dịch chuyển ({period_label})', 'Giá trị hiện tại', f'Giá trị {reference_label}']
        st.dataframe(display_df, use_container_width=True, hide_index=True)

def show_checkin_analysis(period_checkins: List[Dict], overall_checkins: List[Dict], 
                         last_friday: datetime, quarter_start: datetime):
    """Show checkin behavior analysis"""
    period_df = pd.DataFrame(period_checkins)
    overall_df = pd.DataFrame(overall_checkins)
    
    # Period analysis
    st.subheader(f"📅 Period Analysis ({quarter_start.strftime('%d/%m/%Y')} - {last_friday.strftime('%d/%m/%Y')})")
    _display_period_checkin_metrics(period_checkins)
    _display_checkin_distribution_chart(period_checkins)
    
    # Overall analysis
    st.subheader("🏆 Most Active (Overall)")
    _display_overall_checkin_analysis(overall_checkins, quarter_start)

def _display_period_checkin_metrics(period_checkins: List[Dict]):
    """Display period checkin metrics"""
    col1, col2, col3, col4 = st.columns(4)
    
    active_users = len([u for u in period_checkins if u['checkin_count_period'] > 0])
    avg_checkins = np.mean([u['checkin_count_period'] for u in period_checkins]) if period_checkins else 0
    max_checkins = max([u['checkin_count_period'] for u in period_checkins]) if period_checkins else 0
    avg_rate = np.mean([u['checkin_rate_period'] for u in period_checkins]) if period_checkins else 0
    
    with col1:
        total_users = len(period_checkins)
        st.metric("Active Users", active_users, delta=f"{active_users/total_users*100:.1f}%" if total_users > 0 else "0%")
    
    with col2:
        st.metric("Avg Checkins/User", f"{avg_checkins:.1f}")
    
    with col3:
        st.metric("Max Checkins", max_checkins)
    
    with col4:
        st.metric("Avg Checkin Rate", f"{avg_rate:.1f}%")

def _display_checkin_distribution_chart(period_checkins: List[Dict]):
    """Display checkin distribution chart"""
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

def _display_overall_checkin_analysis(overall_checkins: List[Dict], quarter_start: datetime):
    """Display overall checkin analysis"""
    overall_df = pd.DataFrame(overall_checkins)
    
    today = datetime.now()
    weeks_in_quarter = max((today - quarter_start).days / 7, 1)
    
    # Calculate last week boundaries
    days_since_monday = today.weekday()
    monday_this_week = today - timedelta(days=days_since_monday)
    monday_last_week = monday_this_week - timedelta(days=7)
    sunday_last_week = monday_last_week + timedelta(days=6)
    
    st.info(f"📅 Tuần trước: {monday_last_week.strftime('%d/%m/%Y')} - {sunday_last_week.strftime('%d/%m/%Y')}")
    st.info(f"📊 Tần suất checkin = Tổng checkin ÷ {weeks_in_quarter:.1f} tuần (từ đầu quý đến nay)")
    
    # Display table - all employees sorted by total checkins
    all_overall = overall_df.sort_values('total_checkins', ascending=False).copy()
    display_df = all_overall[[
        'user_name', 'total_checkins', 'checkin_frequency_per_week', 'last_week_checkins'
    ]].copy()
    
    display_df.columns = ['👤 Nhân viên', '📊 Tổng checkin', '⚡ Tần suất/tuần (quý)', '📅 Checkin tuần trước']
    display_df['⚡ Tần suất/tuần (quý)'] = display_df['⚡ Tần suất/tuần (quý)'].round(2)
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Summary metrics
    _display_overall_checkin_summary_metrics(overall_df, quarter_start)

def _display_overall_checkin_summary_metrics(overall_df: pd.DataFrame, quarter_start: datetime):
    """Display overall checkin summary metrics"""
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

def show_export_options(df: pd.DataFrame, okr_shifts: List, okr_shifts_monthly: List, 
                       period_checkins: List, overall_checkins: List, analyzer):
    """Show data export options"""
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    export_configs = [
        (col1, "📊 Export Full Dataset", df, "okr_full_dataset"),
        (col2, "🎯 Export Weekly OKR Shifts", pd.DataFrame(okr_shifts), "okr_shifts_weekly"),
        (col4, "📝 Export Period Checkins", pd.DataFrame(period_checkins), "period_checkins"),
        (col5, "📈 Export Overall Checkins", pd.DataFrame(overall_checkins), "overall_checkins"),
        (col6, "👥 Export Filtered Members", analyzer.filtered_members_df, "filtered_members")
    ]
    
    for col, label, data, filename_prefix in export_configs:
        with col:
            if st.button(label):
                if data is not None and not data.empty:
                    csv = data.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
    
    # Monthly export (conditional)
    with col3:
        if okr_shifts_monthly and st.button("🗓️ Export Monthly OKR Shifts"):
            csv = pd.DataFrame(okr_shifts_monthly).to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"okr_shifts_monthly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

def run_analysis(analyzer, selected_cycle: Dict, show_missing_analysis: bool):
    """Run the main analysis"""
    st.header(f"📊 Analysis Results for {selected_cycle['name']}")
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(message, progress):
        status_text.text(message)
        progress_bar.progress(progress)
    
    try:
        # Load data
        df = analyzer.load_and_process_data(update_progress)
        
        if df is None or df.empty:
            st.error("❌ Failed to load data. Please check your API tokens and try again.")
            return
            
        progress_bar.empty()
        status_text.empty()
        
        # Run analysis sections
        show_data_summary(df, analyzer)
        
        if show_missing_analysis:
            st.subheader("🚨 Missing Goals & Checkins Analysis")
            with st.spinner("Analyzing missing goals and checkins..."):
                show_missing_analysis_section(analyzer)
        
        # User Score Analysis
        st.subheader("🏆 User Score Analysis (Monthly OKR Integration)")
        with st.spinner("Calculating user scores with monthly OKR movement..."):
            show_user_score_analysis(analyzer)
        
        # Weekly OKR Analysis
        st.subheader("🎯 Weekly OKR Shift Analysis")
        with st.spinner("Calculating weekly OKR shifts..."):
            okr_shifts = analyzer.calculate_okr_shifts_by_user()
        
        if okr_shifts:
            show_okr_analysis(okr_shifts, DateUtils.get_last_friday_date(), "weekly")
        else:
            st.warning("No weekly OKR shift data available")
        
        # Monthly OKR Analysis
        okr_shifts_monthly = []
        if DateUtils.should_calculate_monthly_shift():
            st.subheader("🗓️ Monthly OKR Shift Analysis")
            with st.spinner("Calculating monthly OKR shifts..."):
                okr_shifts_monthly = analyzer.calculate_okr_shifts_by_user_monthly()
            
            if okr_shifts_monthly:
                show_okr_analysis(okr_shifts_monthly, DateUtils.get_last_month_end_date(), "monthly")
            else:
                st.warning("No monthly OKR shift data available")
        else:
            current_month = datetime.now().month
            quarter_months = {1: "Q1", 4: "Q2", 7: "Q3", 10: "Q4"}
            st.info(f"ℹ️ Monthly OKR shift analysis is not calculated for month {current_month} (start of {quarter_months.get(current_month, 'quarter')})")
        
        # Checkin Analysis
        st.subheader("📝 Checkin Behavior Analysis")
        with st.spinner("Analyzing checkin behavior..."):
            period_checkins, overall_checkins = analyzer.analyze_checkin_behavior()
        
        if period_checkins and overall_checkins:
            show_checkin_analysis(period_checkins, overall_checkins, DateUtils.get_last_friday_date(), DateUtils.get_quarter_start_date())
        else:
            st.warning("No checkin data available")
        
        # Export options
        st.subheader("💾 Export Data")
        show_export_options(df, okr_shifts, okr_shifts_monthly, period_checkins, overall_checkins, analyzer)
        
        st.success("✅ Analysis completed successfully!")
        
    except Exception as e:
        st.error(f"❌ Analysis failed: {e}")
    finally:
        progress_bar.empty()
        status_text.empty()

def send_email_report(analyzer, email_generator: EmailReportGenerator, selected_cycle: Dict, 
                     email_from: str, email_password: str, email_to: str):
    """Send single email report including monthly data when applicable"""
    st.header("📧 Sending Email Report")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(message, progress):
        status_text.text(message)
        progress_bar.progress(progress)
    
    try:
        # Load data
        update_progress("Loading data for email report...", 0.1)
        df = analyzer.load_and_process_data(update_progress)
        
        if df is None or df.empty:
            st.error("❌ Failed to load data for email report")
            return
        
        # Analyze data
        update_progress("Analyzing missing goals and checkins...", 0.25)
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        
        update_progress("Calculating weekly OKR shifts...", 0.4)
        okr_shifts = analyzer.calculate_okr_shifts_by_user()
        
        # Calculate monthly if applicable
        okr_shifts_monthly = []
        if DateUtils.should_calculate_monthly_shift():
            update_progress("Calculating monthly OKR shifts...", 0.55)
            okr_shifts_monthly = analyzer.calculate_okr_shifts_by_user_monthly()
        
        # Create and send email
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
        
        # Display results
        if success:
            st.success(f"✅ {message}")
            monthly_note = " (bao gồm phân tích tháng)" if okr_shifts_monthly else ""
            st.info(f"📧 Email report sent to: {email_to}{monthly_note}")
            
            if st.checkbox("📋 Show email preview", value=False):
                st.subheader("Email Preview")
                st.components.v1.html(html_content, height=600, scrolling=True)
        else:
            st.error(f"❌ {message}")
            
    except Exception as e:
        st.error(f"❌ Error sending email report: {e}")
    finally:
        progress_bar.empty()
        status_text.empty()

def send_email_report_enhanced(analyzer, email_generator: EmailReportGenerator, selected_cycle: Dict,
                              email_from: str, email_password: str, recipient_option: str, 
                              selected_okr_emails: Optional[List[str]] = None):
    """Enhanced email sending with bulk capability and Excel attachment"""
    st.header("📧 Sending Enhanced Email Report")
    
    # Determine recipients
    recipients = _get_email_recipients(analyzer, recipient_option, selected_okr_emails)
    if not recipients:
        return
    
    # Display recipient count với thông tin Excel
    if recipient_option == "okr_users":
        st.info(f"📧 Sending to {len(recipients)} total users who have OKRs (legacy option)")
        st.info("📎 Excel attachment will be included for all recipients")
        # Show first few emails for verification
        if len(recipients) > 0:
            sample_emails = recipients[:3] + (["..."] if len(recipients) > 3 else [])
            st.info(f"📋 Sample recipients: {', '.join(sample_emails)}")
    elif recipient_option == "select_okr_users":
        st.info(f"📧 Sending to {len(recipients)} selected OKR users with Excel")
    elif recipient_option == "all":
        st.info(f"📧 Sending to {len(recipients)} all filtered members")
    else:  # special
        st.info(f"📧 Sending to {len(recipients)} special recipients")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Load and analyze data (reuse existing logic but simplified)
        status_text.text("Loading data...")
        progress_bar.progress(0.1)
        
        df = analyzer.load_and_process_data()
        if df is None or df.empty:
            st.error("❌ Failed to load data for email report")
            return
        
        # Get analysis data
        status_text.text("Analyzing data...")
        progress_bar.progress(0.4)
        
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        okr_shifts = analyzer.calculate_okr_shifts_by_user()
        okr_shifts_monthly = analyzer.calculate_okr_shifts_by_user_monthly() if DateUtils.should_calculate_monthly_shift() else []
        
        # Create Excel for recipients
        status_text.text("Creating Excel report...")
        progress_bar.progress(0.6)
        
        excel_buffer = _create_excel_report(analyzer)
        
        # Create email content
        status_text.text("Creating email content...")
        progress_bar.progress(0.7)
        
        html_content = email_generator.create_email_content(
            analyzer, selected_cycle, members_without_goals, members_without_checkins,
            members_with_goals_no_checkins, okr_shifts, okr_shifts_monthly
        )
        
        # Send emails
        status_text.text("Sending emails...")
        progress_bar.progress(0.8)
        
        subject = f"📊 Báo cáo tiến độ OKR & Checkin - {selected_cycle['name']} - {datetime.now().strftime('%d/%m/%Y')}"
        
        # Đính kèm Excel cho OKR users hoặc special recipients
        attach_excel = recipient_option in ["okr_users", "select_okr_users", "special", "all_with_goals"]
        
        success, message, errors = email_generator.send_email_report_bulk(
            email_from, email_password, recipients, subject, html_content,
            attach_excel=attach_excel, excel_buffer=excel_buffer,
            excel_filename=f"okr_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        
        # Display results
        progress_bar.progress(1.0)
        if success:
            st.success(f"✅ {message}")
            st.info(f"📧 Sent to {len(recipients)} recipients")
            if attach_excel:
                st.info("📎 Excel report attached to all emails")
            
            if errors:
                st.warning("⚠️ Some emails failed:")
                for error in errors:
                    st.text(f"- {error}")
        else:
            st.error(f"❌ {message}")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
    finally:
        progress_bar.empty()
        status_text.empty()


def _get_email_recipients(analyzer, recipient_option: str, selected_okr_emails: Optional[List[str]] = None) -> List[str]:
    """Get email recipients based on option"""
    if recipient_option == "all":
        recipients = get_email_list(analyzer)
        if not recipients:
            st.error("No email addresses found in member data")
            return []
    elif recipient_option == "special":
        recipients = get_default_recipients()
    elif recipient_option == "all_with_goals":
        # Lấy email của tất cả members, ưu tiên OKR users nếu có data
        recipients = get_emails_of_okr_users(analyzer)
        if not recipients:
            recipients = get_email_list(analyzer)
            if not recipients:
                st.error("No email addresses found in member data")
                return []
    elif recipient_option == "okr_users":
        recipients = get_emails_of_total_users_with_okr(analyzer)
        if not recipients:
            st.warning("No OKR user emails found in total users")
            st.error("No email addresses found in total user data")
            return []
    elif recipient_option == "select_okr_users":
        if not selected_okr_emails:
            st.error("No OKR users selected")
            return []
        recipients = selected_okr_emails
    else:
        st.error("Invalid recipient option")
        return []
    
    return recipients

def _create_excel_report(analyzer) -> BytesIO:
    """Create Excel report for email attachment"""
    user_manager = create_user_manager_with_monthly_calculation(analyzer)
    user_manager.update_checkins()
    user_manager.update_okr_movement()
    user_manager.calculate_scores()
    users = user_manager.get_users()
    
    # Validation - đảm bảo Excel có cùng số lượng users với OKR analysis
    if analyzer.final_df is not None and not analyzer.final_df.empty:
        excel_user_count = len(users)
        okr_user_count = len(set(analyzer.final_df['goal_user_name'].dropna().unique()))
        
        if excel_user_count != okr_user_count:
            st.warning(f"⚠️ Excel export mismatch: Excel has {excel_user_count} users, OKR analysis has {okr_user_count} users")
        else:
            st.info(f"✅ Excel export consistency: {excel_user_count} users (matching OKR analysis)")
    
    wb = export_to_excel(users)
    excel_buffer = BytesIO()
    wb.save(excel_buffer)
    excel_buffer.seek(0)
    
    return excel_buffer

def setup_sidebar_configuration():
    """Setup sidebar configuration"""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Token status
        st.subheader("🔑 API Token Status")
        goal_token = os.getenv("GOAL_ACCESS_TOKEN")
        account_token = os.getenv("ACCOUNT_ACCESS_TOKEN")
        
        if goal_token:
            st.success("✅ Goal Access Token: Loaded")
        else:
            st.error("❌ Goal Access Token: Missing")
            
        if account_token:
            st.success("✅ Account Access Token: Loaded")
        else:
            st.error("❌ Account Access Token: Missing")
        
        return goal_token, account_token

def get_emails_of_okr_users(analyzer) -> List[str]:
    """Get email list of users who have OKRs"""
    try:
        if analyzer.filtered_members_df is None:
            return []
        
        # Check email column
        if 'email' not in analyzer.filtered_members_df.columns:
            return []
        
        # Get all valid emails from filtered members first
        all_member_emails = []
        for _, member in analyzer.filtered_members_df.iterrows():
            email = member.get('email', '')
            if pd.notna(email) and str(email).strip() and '@' in str(email):
                all_member_emails.append(str(email).strip())
        
        # If final_df is not loaded yet, return all valid member emails
        # (This is better than returning empty list)
        if analyzer.final_df is None or analyzer.final_df.empty:
            return all_member_emails
        
        # Get users who have goals/OKRs
        users_with_goals = set(analyzer.final_df['goal_user_name'].dropna().unique())
        
        # Match by name and get emails
        okr_users_emails = []
        for _, member in analyzer.filtered_members_df.iterrows():
            member_name = member.get('name', '')
            member_email = member.get('email', '')
            
            if (member_name in users_with_goals and 
                pd.notna(member_email) and 
                str(member_email).strip() and 
                '@' in str(member_email)):
                
                okr_users_emails.append(str(member_email).strip())
        
        # Return OKR user emails if found, otherwise all member emails
        return okr_users_emails if okr_users_emails else all_member_emails
        
    except Exception as e:
        return []

def setup_cycle_selection(analyzer) -> Dict:
    """Setup cycle selection in sidebar"""
    with st.sidebar:
        st.subheader("📅 Cycle Selection")
        
        with st.spinner("🔄 Loading available cycles..."):
            cycles = analyzer.get_cycle_list()

        if not cycles:
            st.error("❌ Could not load cycles. Please check your API tokens and connection.")
            return None

        cycle_options = {f"{cycle['name']} ({cycle['formatted_start_time']})": cycle for cycle in cycles}
        selected_cycle_name = st.selectbox(
            "Select Cycle",
            options=list(cycle_options.keys()),
            index=0,
            help="Choose the quarterly cycle to analyze"
        )
        
        selected_cycle = cycle_options[selected_cycle_name]
        analyzer.checkin_path = selected_cycle['path']
        
        # Auto-load filtered members để tránh lỗi trong sidebar
        if analyzer.filtered_members_df is None:
            with st.spinner("🔄 Loading filtered members..."):
                try:
                    analyzer.get_filtered_members()
                    st.success(f"✅ Loaded {len(analyzer.filtered_members_df)} filtered members")
                except Exception as e:
                    st.error(f"❌ Failed to load filtered members: {e}")
        
        st.info(f"🎯 **Selected Cycle:**\n\n**{selected_cycle['name']}**\n\nPath: `{selected_cycle['path']}`\n\nStart: {selected_cycle['formatted_start_time']}")
        
        return selected_cycle

def get_all_member_emails_count(analyzer) -> int:
    """Get count of all member emails for sidebar display"""
    try:
        if analyzer.filtered_members_df is None:
            return 0
        
        if 'email' not in analyzer.filtered_members_df.columns:
            return 0
        
        valid_email_count = 0
        for _, member in analyzer.filtered_members_df.iterrows():
            email = member.get('email', '')
            if pd.notna(email) and str(email).strip() and '@' in str(email):
                valid_email_count += 1
        
        return valid_email_count
        
    except Exception:
        return 0

def get_total_user_emails_count(analyzer) -> int:
    """Get count of all valid email addresses in total account users (not filtered)"""
    try:
        total_users_df = analyzer.get_total_account_users()
        if total_users_df is None or total_users_df.empty:
            return 0
        
        if 'email' not in total_users_df.columns:
            return 0
        
        valid_emails = 0
        for _, user in total_users_df.iterrows():
            user_email = user.get('email', '')
            if pd.notna(user_email) and str(user_email).strip() and '@' in str(user_email):
                valid_emails += 1
        
        return valid_emails
    except Exception as e:
        return 0

def get_emails_of_total_users_with_okr(analyzer) -> List[str]:
    """Get email list of total users (not filtered) who have OKRs"""
    try:
        # Get total account users
        total_users_df = analyzer.get_total_account_users()
        if total_users_df is None or total_users_df.empty:
            return []
        
        if 'email' not in total_users_df.columns:
            return []
        
        # Get all valid emails from total users
        all_total_emails = []
        for _, user in total_users_df.iterrows():
            user_email = user.get('email', '')
            if pd.notna(user_email) and str(user_email).strip() and '@' in str(user_email):
                all_total_emails.append(str(user_email).strip())
        
        # If no OKR data loaded yet, return all total emails
        if analyzer.final_df is None or analyzer.final_df.empty:
            return all_total_emails
        
        # Get unique users who have goals
        users_with_goals = set()
        for _, row in analyzer.final_df.iterrows():
            goal_user_name = row.get('goal_user_name', '')
            if goal_user_name:
                users_with_goals.add(goal_user_name)
        
        # Match by name and get emails from total users
        okr_users_emails = []
        for _, user in total_users_df.iterrows():
            user_name = user.get('name', '')
            user_email = user.get('email', '')
            
            if (user_name in users_with_goals and 
                pd.notna(user_email) and 
                str(user_email).strip() and
                '@' in str(user_email)):
                
                okr_users_emails.append(str(user_email).strip())
        
        # Return OKR user emails if found, otherwise all total emails
        return okr_users_emails if okr_users_emails else all_total_emails
        
    except Exception as e:
        return []

def setup_analysis_options():
    """Setup analysis options in sidebar"""
    with st.sidebar:
        st.subheader("📊 Analysis Options")
        return st.checkbox("Show Missing Goals & Checkins Analysis", value=True)



def setup_enhanced_email_configuration(analyzer):
    """Setup enhanced email configuration in sidebar"""
    with st.sidebar:
        st.subheader("📧 Enhanced Email Settings")
        
        # Recipient options - mặc định chọn all_with_goals
        recipient_option = st.radio(
            "Send emails to:",
            ["all_with_goals", "special", "all", "okr_users"],
            format_func=lambda x: {
                "special": "Special recipients only (xnk3)",
                "all": "All filtered members",
                "all_with_goals": "All members with goals (default - with Excel)",
                "okr_users": "People with OKRs (legacy option)"
            }[x],
            index=0  # Mặc định chọn all_with_goals
        )
        
        # Display recipient info với analyzer
        _display_recipient_info_with_count(recipient_option, analyzer)
        
        return recipient_option, None

def _display_recipient_info_with_count(recipient_option: str, analyzer=None, selected_okr_emails: Optional[List[str]] = None):
    """Display recipient information with email count"""
    if recipient_option == "all":
        if analyzer and analyzer.filtered_members_df is not None:
            email_count = get_all_member_emails_count(analyzer)
            st.info(f"📊 Will send to {email_count} filtered members")
        else:
            st.info("📊 Will send to all filtered members")
    elif recipient_option == "special":
        st.info("📊 Will send to 1 special recipient with Excel attachment")
    elif recipient_option == "all_with_goals":
        if analyzer:
            try:
                # Đếm tổng số email hợp lệ
                total_email_count = get_all_member_emails_count(analyzer)
                
                if total_email_count > 0:
                    st.success(f"📧 Found {total_email_count} email addresses for All members with goals")
                    st.info("📎 Excel attachment will be included for all recipients")
                    st.info(f"📋 Will send to all {total_email_count} members (OKR filtering will be applied if data is loaded)")
                else:
                    st.warning("⚠️ Found 0 valid email addresses in filtered members")
                    
            except Exception as e:
                st.error(f"Error counting emails: {e}")
                st.info("📧 Email count will be calculated when running")
        else:
            st.info("📧 Email count will be calculated when running")
    elif recipient_option == "okr_users":
        if analyzer:
            try:
                total_email_count = get_total_user_emails_count(analyzer)
                st.info(f"📊 Will send to people who have OKRs with Excel attachment")
                st.success(f"📧 Available emails in total users: {total_email_count}")
                st.info("📧 Will send to total users (not filtered) who have OKRs")
                    
            except Exception as e:
                st.error(f"Error counting emails: {e}")
                st.info("📧 Email count will be calculated when running")
        else:
            st.info("📧 Email count will be calculated when running")
    elif recipient_option == "select_okr_users":
        if selected_okr_emails:
            st.info(f"📊 Will send to {len(selected_okr_emails)} selected OKR users")
        else:
            st.warning("No OKR users selected")

def main():
    """Main application entry point"""
    st.title("🎯 OKR & Checkin Analysis Dashboard")
    st.markdown("---")

    # Setup configuration
    goal_token, account_token = setup_sidebar_configuration()
    
    if not goal_token or not account_token:
        st.error("❌ API tokens not found in environment variables. Please set GOAL_ACCESS_TOKEN and ACCOUNT_ACCESS_TOKEN.")
        st.info("Make sure to set the following environment variables:")
        st.code("""
GOAL_ACCESS_TOKEN=your_goal_token_here
ACCOUNT_ACCESS_TOKEN=your_account_token_here
        """)
        return

    # Initialize system
    try:
        analyzer = OKRAnalysisSystem(goal_token, account_token)
        email_generator = EmailReportGenerator()
        # Lưu analyzer vào session state để sử dụng trong sidebar
        st.session_state.analyzer = analyzer
    except Exception as e:
        st.error(f"Failed to initialize analyzer: {e}")
        return

    # Setup sidebar options
    selected_cycle = setup_cycle_selection(analyzer)
    if not selected_cycle:
        return
    
    show_missing_analysis = setup_analysis_options()
    recipient_option, custom_emails = setup_enhanced_email_configuration(analyzer)

    # Auto-run analysis để đảm bảo data được load
    auto_run_key = f"auto_analysis_{selected_cycle['path']}"
    if auto_run_key not in st.session_state:
        st.session_state[auto_run_key] = True
        with st.spinner("🚀 Auto-running analysis..."):
            run_analysis(analyzer, selected_cycle, show_missing_analysis)
    
    # Main action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        analyze_button = st.button("🔄 Re-run Analysis", type="primary", use_container_width=True)
    
    with col2:
        # Thay đổi tên nút để phản ánh việc gửi Excel
        if recipient_option == "okr_users":
            button_text = "📧 Send Email Report + Excel to OKR Users"
        else:
            button_text = "📧 Send Enhanced Email Report"
        email_button = st.button(button_text, type="secondary", use_container_width=True)
    
    # Handle button clicks
    if analyze_button:
        run_analysis(analyzer, selected_cycle, show_missing_analysis)
    
    if email_button:
        send_email_report_enhanced(
            analyzer, email_generator, selected_cycle, 
            "apluscorp.hr@gmail.com", 'mems nctq yxss gruw', 
            recipient_option, custom_emails
        )


if __name__ == "__main__":
    main()

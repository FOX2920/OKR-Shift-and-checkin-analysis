"""
API Client cho OKR Analysis System
"""
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from typing import Dict, List
from utils import DateUtils


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

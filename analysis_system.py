"""
Main OKR Analysis System combining all components
"""
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from api_client import APIClient
from data_processor import DataProcessor
from okr_calculator import OKRCalculator
from utils import DateUtils


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

    # Additional data access methods
    def fetch_account_data(self) -> pd.DataFrame:
        """Fetch account data for UserManager"""
        return self.api_client.get_account_users()

    def fetch_krs_data(self) -> pd.DataFrame:
        """Fetch KRs data for UserManager"""
        return self.api_client.get_krs_data(self.checkin_path)

    def fetch_checkin_data(self) -> pd.DataFrame:
        """Fetch checkin data for UserManager"""
        all_checkins = self.api_client.get_all_checkins(self.checkin_path)
        return self.data_processor.extract_checkin_data(all_checkins)

    def fetch_cycle_data(self) -> pd.DataFrame:
        """Fetch cycle data for UserManager"""
        # This would be implemented if needed
        return pd.DataFrame()

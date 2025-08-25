"""
Model classes cho OKR Analysis System
"""
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Optional, List
import pandas as pd
import numpy as np
from utils import get_current_quarter_start


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

    def calculate_scores(self):
        """Calculate score for all users."""
        for user in self.users.values():
            user.calculate_score()

    def get_users(self):
        """Return list of all users."""
        return list(self.users.values())

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


def create_user_manager_with_monthly_calculation(analyzer):
    """Create UserManager integrated with monthly OKR calculation from OKRAnalysisSystem"""
    try:
        # Get necessary dataframes from analyzer
        account_df = analyzer.fetch_account_data()
        krs_df = analyzer.fetch_krs_data()
        checkin_df = analyzer.fetch_checkin_data()
        cycle_df = analyzer.fetch_cycle_data()
        
        # Get final_df for monthly calculations
        final_df = analyzer.final_df
        
        # Create UserManager with all necessary data
        user_manager = UserManager(
            account_df=account_df,
            krs_df=krs_df, 
            checkin_df=checkin_df,
            cycle_df=cycle_df,
            final_df=final_df
        )
        
        # Calculate checkins and scores
        user_manager.update_checkins()
        user_manager.update_okr_movement()  # Uses integrated monthly calculation
        user_manager.calculate_scores()
        
        return user_manager
        
    except Exception as e:
        print(f"Error creating user manager: {e}")
        return None

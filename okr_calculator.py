"""
OKR Calculator classes cho OKR Analysis System
"""
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime
from typing import Tuple, List, Dict
from utils import DateUtils


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

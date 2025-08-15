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

# THAY THẾ TỪ DÒNG IMPORT ĐẾN HẾT CLASS PDFReportGenerator

# Thay vì import ReportLab, sử dụng matplotlib
import io
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

class PDFReportGenerator:
    """Generate PDF reports for OKR analysis using matplotlib instead of ReportLab"""
    
    def __init__(self):
        # Set up matplotlib for Vietnamese text support
        plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial Unicode MS', 'SimHei', 'sans-serif']
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.unicode_minus'] = False
        
    def register_vietnamese_fonts(self):
        """Dummy method for compatibility - no longer needed with matplotlib"""
        pass
    
    def setup_custom_styles(self):
        """Dummy method for compatibility - no longer needed with matplotlib"""
        pass
        
    def create_pdf_report(self, analyzer, selected_cycle, members_without_goals, members_without_checkins, 
                         members_with_goals_no_checkins, okr_shifts):
        """Create comprehensive PDF report using matplotlib"""
        
        buffer = io.BytesIO()
        
        with PdfPages(buffer) as pdf:
            # Page 1: Title and Summary
            self._create_title_page(pdf, selected_cycle, analyzer, members_without_goals, 
                                  members_without_checkins, members_with_goals_no_checkins, okr_shifts)
            
            # Page 2: Charts and Analysis
            self._create_charts_page(pdf, analyzer, members_without_goals, members_without_checkins, okr_shifts)
            
            # Page 3: Detailed Tables
            self._create_tables_page(pdf, members_without_goals, members_without_checkins, okr_shifts)
            
            # Page 4: Checkin Analysis
            self._create_checkin_page(pdf, analyzer)
        
        buffer.seek(0)
        return buffer
    
    def _create_title_page(self, pdf, selected_cycle, analyzer, members_without_goals, 
                          members_without_checkins, members_with_goals_no_checkins, okr_shifts):
        """Create title page with summary statistics"""
        
        fig, ax = plt.subplots(figsize=(8.27, 11.69))  # A4 size
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 14)
        ax.axis('off')
        
        current_date = datetime.now().strftime("%d/%m/%Y")
        
        # Title
        ax.text(5, 13, 'BÁO CÁO TIẾN ĐỘ OKR & CHECKIN', 
                fontsize=20, fontweight='bold', ha='center', color='#2c3e50')
        ax.text(5, 12.3, f'{selected_cycle["name"]}', 
                fontsize=16, fontweight='bold', ha='center', color='#3498db')
        ax.text(5, 11.8, f'Ngày báo cáo: {current_date}', 
                fontsize=12, ha='center', color='#7f8c8d')
        
        # Add line separator
        ax.plot([1, 9], [11.3, 11.3], color='#3498db', linewidth=2)
        
        # Summary statistics
        total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
        members_with_goals = total_members - len(members_without_goals)
        members_with_checkins = total_members - len(members_without_checkins)
        
        progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0]) if okr_shifts else 0
        stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0]) if okr_shifts else 0
        issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0]) if okr_shifts else 0
        
        # Create summary boxes
        y_start = 10.5
        box_height = 0.8
        box_width = 7
        
        # Summary data
        summary_data = [
            ('TỔNG QUAN', '#3498db', [
                f'Tổng nhân viên: {total_members}',
                f'Có OKR: {members_with_goals} ({(members_with_goals/total_members*100):.1f}%)' if total_members > 0 else 'Có OKR: 0',
                f'Có Checkin: {members_with_checkins} ({(members_with_checkins/total_members*100):.1f}%)' if total_members > 0 else 'Có Checkin: 0'
            ]),
            ('PHÂN TÍCH TIẾN ĐỘ', '#27AE60', [
                f'Nhân viên tiến bộ: {progress_users}',
                f'Nhân viên ổn định: {stable_users}',
                f'Nhân viên cần hỗ trợ: {issue_users}'
            ])
        ]
        
        y_pos = y_start
        for title, color, items in summary_data:
            # Draw box
            rect = patches.Rectangle((1.5, y_pos - box_height), box_width, box_height, 
                                   linewidth=2, edgecolor=color, facecolor=color, alpha=0.1)
            ax.add_patch(rect)
            
            # Title
            ax.text(5, y_pos - 0.2, title, fontsize=14, fontweight='bold', 
                   ha='center', color=color)
            
            # Items
            for i, item in enumerate(items):
                ax.text(2, y_pos - 0.5 - (i * 0.15), f'• {item}', fontsize=11, color='#2c3e50')
            
            y_pos -= 2
        
        # Key insights box
        insights_y = 6
        rect = patches.Rectangle((1.5, insights_y - 2), box_width, 1.8, 
                               linewidth=2, edgecolor='#e74c3c', facecolor='#e74c3c', alpha=0.1)
        ax.add_patch(rect)
        
        ax.text(5, insights_y - 0.2, 'ĐIỂM CẦN QUAN TÂM', fontsize=14, fontweight='bold', 
               ha='center', color='#e74c3c')
        
        insights = [
            f'Nhân viên chưa có OKR: {len(members_without_goals)} người',
            f'Nhân viên chưa checkin: {len(members_without_checkins)} người',
            f'Có OKR nhưng chưa checkin: {len(members_with_goals_no_checkins)} người'
        ]
        
        for i, insight in enumerate(insights):
            ax.text(2, insights_y - 0.7 - (i * 0.3), f'⚠️ {insight}', fontsize=11, color='#e74c3c')
        
        # Footer
        ax.text(5, 1, 'A Plus Mineral Material Corporation', 
                fontsize=14, fontweight='bold', ha='center', color='#2c3e50')
        ax.text(5, 0.6, 'Báo cáo được tạo tự động bởi hệ thống OKR Analysis', 
                fontsize=10, ha='center', color='#7f8c8d')
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def _create_charts_page(self, pdf, analyzer, members_without_goals, members_without_checkins, okr_shifts):
        """Create page with charts and visualizations"""
        
        fig = plt.figure(figsize=(8.27, 11.69))
        
        # Chart 1: Goal Distribution (Pie Chart)
        ax1 = plt.subplot(3, 2, 1)
        total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
        members_with_goals = total_members - len(members_without_goals)
        
        if total_members > 0:
            sizes = [members_with_goals, len(members_without_goals)]
            labels = ['Có OKR', 'Chưa có OKR']
            colors = ['#27AE60', '#E74C3C']
            
            wedges, texts, autotexts = ax1.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                             colors=colors, startangle=90)
            ax1.set_title('Phân bố trạng thái OKR', fontweight='bold', pad=20)
        
        # Chart 2: Checkin Distribution (Pie Chart)
        ax2 = plt.subplot(3, 2, 2)
        members_with_checkins = total_members - len(members_without_checkins)
        
        if total_members > 0:
            sizes = [members_with_checkins, len(members_without_checkins)]
            labels = ['Có Checkin', 'Chưa có Checkin']
            colors = ['#3498DB', '#F39C12']
            
            wedges, texts, autotexts = ax2.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                             colors=colors, startangle=90)
            ax2.set_title('Phân bố trạng thái Checkin', fontweight='bold', pad=20)
        
        # Chart 3: OKR Shifts Bar Chart
        ax3 = plt.subplot(3, 1, 2)
        if okr_shifts:
            top_shifts = okr_shifts[:15]  # Top 15
            names = [u['user_name'][:15] + '...' if len(u['user_name']) > 15 else u['user_name'] 
                    for u in top_shifts]
            values = [u['okr_shift'] for u in top_shifts]
            
            colors = ['#27AE60' if v > 0 else '#E74C3C' if v < 0 else '#F39C12' for v in values]
            
            bars = ax3.bar(range(len(names)), values, color=colors)
            ax3.set_xticks(range(len(names)))
            ax3.set_xticklabels(names, rotation=45, ha='right')
            ax3.set_title('Dịch chuyển OKR (Top 15)', fontweight='bold', pad=20)
            ax3.set_ylabel('Dịch chuyển OKR')
            ax3.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for bar, value in zip(bars, values):
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + (0.01 if height >= 0 else -0.05),
                        f'{value:.2f}', ha='center', va='bottom' if height >= 0 else 'top', fontsize=8)
        
        # Chart 4: Progress Distribution
        ax4 = plt.subplot(3, 2, 5)
        if okr_shifts:
            progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0])
            stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0])
            issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0])
            
            sizes = [progress_users, stable_users, issue_users]
            labels = ['Tiến bộ', 'Ổn định', 'Cần hỗ trợ']
            colors = ['#27AE60', '#F39C12', '#E74C3C']
            
            # Filter out zero values
            non_zero_data = [(size, label, color) for size, label, color in zip(sizes, labels, colors) if size > 0]
            if non_zero_data:
                sizes, labels, colors = zip(*non_zero_data)
                wedges, texts, autotexts = ax4.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                                 colors=colors, startangle=90)
                ax4.set_title('Phân bố tiến độ nhân viên', fontweight='bold', pad=20)
        
        plt.tight_layout()
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def _create_tables_page(self, pdf, members_without_goals, members_without_checkins, okr_shifts):
        """Create page with detailed tables"""
        
        fig, ax = plt.subplots(figsize=(8.27, 11.69))
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 14)
        ax.axis('off')
        
        y_pos = 13.5
        
        # Page title
        ax.text(5, y_pos, 'CHI TIẾT PHÂN TÍCH', fontsize=16, fontweight='bold', 
               ha='center', color='#2c3e50')
        y_pos -= 0.8
        
        # Members without goals
        if members_without_goals:
            ax.text(0.5, y_pos, f'NHÂN VIÊN CHƯA CÓ OKR ({len(members_without_goals)} người)', 
                   fontsize=12, fontweight='bold', color='#e74c3c')
            y_pos -= 0.4
            
            # Table header
            ax.text(0.5, y_pos, 'STT', fontsize=10, fontweight='bold')
            ax.text(1.5, y_pos, 'Tên', fontsize=10, fontweight='bold')
            ax.text(4, y_pos, 'Username', fontsize=10, fontweight='bold')
            ax.text(6.5, y_pos, 'Chức vụ', fontsize=10, fontweight='bold')
            
            # Draw header line
            ax.plot([0.5, 9.5], [y_pos - 0.1, y_pos - 0.1], color='#2c3e50', linewidth=1)
            y_pos -= 0.3
            
            # Table rows (limit to first 15)
            for i, member in enumerate(members_without_goals[:15], 1):
                if y_pos < 1:
                    break
                ax.text(0.5, y_pos, str(i), fontsize=9)
                ax.text(1.5, y_pos, member.get('name', '')[:20], fontsize=9)
                ax.text(4, y_pos, member.get('username', ''), fontsize=9)
                ax.text(6.5, y_pos, member.get('job', '')[:25], fontsize=9)
                y_pos -= 0.25
            
            if len(members_without_goals) > 15:
                ax.text(0.5, y_pos, f'... và {len(members_without_goals) - 15} nhân viên khác', 
                       fontsize=9, style='italic', color='#7f8c8d')
            
            y_pos -= 0.5
        
        # Top performers
        if okr_shifts and y_pos > 3:
            top_performers = [u for u in okr_shifts if u['okr_shift'] > 0][:10]
            if top_performers:
                ax.text(0.5, y_pos, f'TOP NHÂN VIÊN TIẾN BỘ ({len(top_performers)} người)', 
                       fontsize=12, fontweight='bold', color='#27AE60')
                y_pos -= 0.4
                
                # Table header
                ax.text(0.5, y_pos, 'STT', fontsize=10, fontweight='bold')
                ax.text(1.5, y_pos, 'Nhân viên', fontsize=10, fontweight='bold')
                ax.text(4.5, y_pos, 'Dịch chuyển', fontsize=10, fontweight='bold')
                ax.text(6.5, y_pos, 'Hiện tại', fontsize=10, fontweight='bold')
                ax.text(8, y_pos, 'Trước đó', fontsize=10, fontweight='bold')
                
                # Draw header line
                ax.plot([0.5, 9.5], [y_pos - 0.1, y_pos - 0.1], color='#2c3e50', linewidth=1)
                y_pos -= 0.3
                
                # Table rows
                for i, user in enumerate(top_performers, 1):
                    if y_pos < 1:
                        break
                    ax.text(0.5, y_pos, str(i), fontsize=9)
                    ax.text(1.5, y_pos, user['user_name'][:20], fontsize=9)
                    ax.text(4.5, y_pos, f"{user['okr_shift']:.2f}", fontsize=9, color='#27AE60')
                    ax.text(6.5, y_pos, f"{user['current_value']:.2f}", fontsize=9)
                    ax.text(8, y_pos, f"{user['last_friday_value']:.2f}", fontsize=9)
                    y_pos -= 0.25
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def _create_checkin_page(self, pdf, analyzer):
        """Create page with checkin analysis"""
        
        # Get checkin behavior data
        period_checkins, overall_checkins = analyzer.analyze_checkin_behavior()
        
        fig, ax = plt.subplots(figsize=(8.27, 11.69))
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 14)
        ax.axis('off')
        
        y_pos = 13.5
        
        # Page title
        ax.text(5, y_pos, 'PHÂN TÍCH HOẠT ĐỘNG CHECKIN', fontsize=16, fontweight='bold', 
               ha='center', color='#2c3e50')
        y_pos -= 0.8
        
        if overall_checkins:
            ax.text(0.5, y_pos, f'TOP NHÂN VIÊN HOẠT ĐỘNG NHẤT ({len(overall_checkins[:20])} người)', 
                   fontsize=12, fontweight='bold', color='#3498db')
            y_pos -= 0.4
            
            # Table header
            ax.text(0.5, y_pos, 'STT', fontsize=10, fontweight='bold')
            ax.text(1.5, y_pos, 'Nhân viên', fontsize=10, fontweight='bold')
            ax.text(4.5, y_pos, 'Tổng checkin', fontsize=10, fontweight='bold')
            ax.text(6.5, y_pos, 'Tần suất/tuần', fontsize=10, fontweight='bold')
            ax.text(8.5, y_pos, 'Tuần trước', fontsize=10, fontweight='bold')
            
            # Draw header line
            ax.plot([0.5, 9.5], [y_pos - 0.1, y_pos - 0.1], color='#2c3e50', linewidth=1)
            y_pos -= 0.3
            
            # Table rows (top 20)
            for i, user in enumerate(overall_checkins[:20], 1):
                if y_pos < 1:
                    break
                ax.text(0.5, y_pos, str(i), fontsize=9)
                ax.text(1.5, y_pos, user['user_name'][:20], fontsize=9)
                ax.text(4.5, y_pos, str(user.get('total_checkins', 0)), fontsize=9)
                ax.text(6.5, y_pos, f"{user.get('checkin_frequency_per_week', 0):.2f}", fontsize=9)
                ax.text(8.5, y_pos, str(user.get('last_week_checkins', 0)), fontsize=9)
                y_pos -= 0.25
        else:
            ax.text(5, y_pos, 'Không có dữ liệu checkin', fontsize=12, ha='center', color='#7f8c8d')
        
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    
    def create_summary_chart(self, data, title, chart_type='bar'):
        """Create matplotlib chart for PDF with Vietnamese font support - kept for compatibility"""
        try:
            # Thiết lập font cho matplotlib để hỗ trợ tiếng Việt
            plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial Unicode MS', 'SimHei']
            
            fig, ax = plt.subplots(figsize=(8, 4))
            
            if chart_type == 'pie' and data:
                labels = list(data.keys())
                sizes = list(data.values())
                colors_pie = ['#27AE60', '#E74C3C', '#3498DB', '#F39C12', '#9B59B6']
                
                wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                                 colors=colors_pie[:len(labels)], startangle=90)
                ax.set_title(title, fontsize=12, fontweight='bold', pad=20)
                
                # Đảm bảo text hiển thị đúng
                for text in texts:
                    text.set_fontsize(10)
                for autotext in autotexts:
                    autotext.set_color('white')
                    autotext.set_fontsize(9)
                    autotext.set_weight('bold')
                
            elif chart_type == 'bar' and data:
                names = list(data.keys())[:15]  # Top 15
                values = list(data.values())[:15]
                
                bars = ax.bar(names, values, color=['#27AE60' if v > 0 else '#E74C3C' if v < 0 else '#F39C12' for v in values])
                ax.set_title(title, fontsize=12, fontweight='bold', pad=20)
                ax.set_xlabel('Nhân viên')
                ax.set_ylabel('Dịch chuyển OKR')
                
                # Rotate x-axis labels
                plt.xticks(rotation=45, ha='right')
                
                # Add value labels on bars
                for bar, value in zip(bars, values):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + (0.01 if height >= 0 else -0.05),
                           f'{value:.2f}', ha='center', va='bottom' if height >= 0 else 'top', fontsize=8)
            
            plt.tight_layout()
            
            # Save to bytes
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
            img_buffer.seek(0)
            plt.close()
            
            return img_buffer
            
        except Exception as e:
            print(f"Error creating chart: {e}")
            return None

class OKRAnalysisSystem:
    """OKR Analysis System for Streamlit"""

    def __init__(self, goal_access_token: str, account_access_token: str):
        self.goal_access_token = goal_access_token
        self.account_access_token = account_access_token
        self.checkin_path = None
        self.final_df = None
        self.filtered_members_df = None

    def get_filtered_members(self) -> pd.DataFrame:
        """Get filtered members from account API"""
        try:
            url = "https://account.base.vn/extapi/v1/group/get"
            data = {
                "access_token": self.account_access_token,
                "path": "aplus"
            }
            
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            response_data = response.json()
            
            # Get members from response
            group = response_data.get('group', {})
            members = group.get('members', [])
            
            # Convert to DataFrame with email field
            df = pd.DataFrame([
                {
                    'id': str(m.get('id', '')),
                    'name': m.get('name', ''),
                    'username': m.get('username', ''),
                    'job': m.get('title', ''),
                    'email': m.get('email', '')  # Added email field
                }
                for m in members
            ])
            
            # Filter out unwanted job titles and specific usernames
            filtered_df = df[~df['job'].str.lower().str.contains('kcs|agile|khu vực|sa ti co|trainer|specialist|no|chuyên gia|xnk|vat|trưởng phòng thị trường', na=False)]
            # Filter out specific username "ThuAn"
            filtered_df = filtered_df[filtered_df['username'] != 'ThuAn']
            
            self.filtered_members_df = filtered_df
            return filtered_df
            
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching account members: {e}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Unexpected error getting members: {e}")
            return pd.DataFrame()

    def convert_timestamp_to_datetime(self, timestamp) -> Optional[str]:
        """Convert timestamp to datetime string"""
        if timestamp is None or timestamp == '' or timestamp == 0:
            return None
        try:
            return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return None

    def get_last_friday_date(self) -> datetime:
        """Get last Friday date - always returns Friday of previous week regardless of current day"""
        today = datetime.now()
        
        # Get current day of week (0=Monday, 6=Sunday)
        current_weekday = today.weekday()
        
        # Calculate days back to Monday of current week
        days_to_monday_current_week = current_weekday
        monday_current_week = today - timedelta(days=days_to_monday_current_week)
        
        # Get Monday of previous week
        monday_previous_week = monday_current_week - timedelta(days=7)
        
        # Get Friday of previous week (Monday + 4 days)
        friday_previous_week = monday_previous_week + timedelta(days=4)
        
        return friday_previous_week

    def get_quarter_start_date(self) -> datetime:
        """Get current quarter start date"""
        today = datetime.now()
        quarter = (today.month - 1) // 3 + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        return datetime(today.year, quarter_start_month, 1)

    def get_cycle_list(self) -> List[Dict]:
        """Get list of quarterly cycles sorted by most recent first"""
        url = "https://goal.base.vn/extapi/v1/cycle/list"
        payload = {'access_token': self.goal_access_token}

        try:
            response = requests.post(url, data=payload, timeout=30)
            response.raise_for_status()
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
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching cycle list: {e}")
            return []
        except Exception as e:
            st.error(f"Unexpected error: {e}")
            return []

    def get_account_users(self) -> pd.DataFrame:
        """Get users from Account API"""
        url = "https://account.base.vn/extapi/v1/users"
        data = {"access_token": self.account_access_token}

        try:
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            json_response = response.json()
            
            if isinstance(json_response, list) and len(json_response) > 0:
                json_response = json_response[0]

            account_users = json_response.get('users', [])
            return pd.DataFrame([{
                'id': str(user['id']),
                'name': user['name'],
                'username': user['username']
            } for user in account_users])
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching account users: {e}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Unexpected error getting users: {e}")
            return pd.DataFrame()

    def get_goals_data(self) -> pd.DataFrame:
        """Get goals data from API"""
        url = "https://goal.base.vn/extapi/v1/cycle/get.full"
        payload = {'access_token': self.goal_access_token, 'path': self.checkin_path}

        try:
            response = requests.post(url, data=payload, timeout=30)
            response.raise_for_status()
            data = response.json()

            goals_data = []
            for goal in data.get('goals', []):
                goal_data = {
                    'goal_id': goal.get('id'),
                    'goal_name': goal.get('name', 'Unknown Goal'),
                    'goal_content': goal.get('content', ''),
                    'goal_since': self.convert_timestamp_to_datetime(goal.get('since')),
                    'goal_current_value': goal.get('current_value', 0),
                    'goal_user_id': str(goal.get('user_id', '')),
                }
                goals_data.append(goal_data)

            return pd.DataFrame(goals_data)
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching goals data: {e}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Unexpected error getting goals: {e}")
            return pd.DataFrame()

    def get_krs_data(self) -> pd.DataFrame:
        """Get KRs data from API with pagination"""
        url = "https://goal.base.vn/extapi/v1/cycle/krs"
        all_krs = []
        page = 1
        max_pages = 50  # Safety limit

        progress_bar = st.progress(0)
        status_text = st.empty()

        while page <= max_pages:
            status_text.text(f"Loading KRs... Page {page}")
            data = {"access_token": self.goal_access_token, "path": self.checkin_path, "page": page}

            try:
                response = requests.post(url, data=data, timeout=30)
                response.raise_for_status()
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
                        'kr_since': self.convert_timestamp_to_datetime(kr.get('since')),
                        'kr_current_value': kr.get('current_value', 0),
                        'kr_user_id': str(kr.get('user_id', '')),
                        'goal_id': kr.get('goal_id'),
                    }
                    all_krs.append(kr_data)

                progress_bar.progress(min(page / 10, 1.0))
                page += 1

            except requests.exceptions.RequestException as e:
                st.error(f"Error at page {page}: {e}")
                break
            except Exception as e:
                st.error(f"Unexpected error at page {page}: {e}")
                break

        progress_bar.empty()
        status_text.empty()
        return pd.DataFrame(all_krs)

    def get_all_checkins(self) -> List[Dict]:
        """Get all checkins with pagination"""
        url = "https://goal.base.vn/extapi/v1/cycle/checkins"
        all_checkins = []
        page = 1
        max_pages = 100  # Safety limit

        progress_bar = st.progress(0)
        status_text = st.empty()

        while page <= max_pages:
            status_text.text(f"Loading checkins... Page {page}")
            data = {"access_token": self.goal_access_token, "path": self.checkin_path, "page": page}

            try:
                response = requests.post(url, data=data, timeout=30)
                response.raise_for_status()
                response_data = response.json()

                if isinstance(response_data, list) and len(response_data) > 0:
                    response_data = response_data[0]

                checkins = response_data.get('checkins', [])
                if not checkins:
                    break

                all_checkins.extend(checkins)
                progress_bar.progress(min(page / 20, 1.0))

                if len(checkins) < 20:  # Last page typically has fewer items
                    break

                page += 1

            except requests.exceptions.RequestException as e:
                st.error(f"Error loading checkins at page {page}: {e}")
                break
            except Exception as e:
                st.error(f"Unexpected error loading checkins at page {page}: {e}")
                break

        progress_bar.empty()
        status_text.empty()
        return all_checkins

    def extract_checkin_data(self, all_checkins: List[Dict]) -> pd.DataFrame:
        """Extract checkin data into DataFrame"""
        checkin_list = []

        for checkin in all_checkins:
            try:
                checkin_id = checkin.get('id', '')
                checkin_name = checkin.get('name', '')
                user_id = str(checkin.get('user_id', ''))
                since_timestamp = checkin.get('since', '')

                # Convert timestamp
                if since_timestamp:
                    try:
                        since_date = datetime.fromtimestamp(int(since_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        since_date = str(since_timestamp)
                else:
                    since_date = ''

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

    def analyze_missing_goals_and_checkins(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Analyze members without goals and without checkins"""
        try:
            if self.filtered_members_df is None or self.final_df is None:
                return [], [], []

            # Get users with goals
            users_with_goals = set(self.final_df['goal_user_name'].dropna().unique())
            
            # Get users with checkins (anyone who has made at least one checkin)
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
                        'email': member_info.get('email', ''),  # Added email field
                        'id': member_info.get('id', '')
                    })
                
                if not has_checkin:
                    members_without_checkins.append({
                        'name': member_name,
                        'username': member_info.get('username', ''),
                        'job': member_info.get('job', ''),
                        'email': member_info.get('email', ''),  # Added email field
                        'id': member_info.get('id', ''),
                        'has_goal': has_goal
                    })
                
                if has_goal and not has_checkin:
                    members_with_goals_no_checkins.append({
                        'name': member_name,
                        'username': member_info.get('username', ''),
                        'job': member_info.get('job', ''),
                        'email': member_info.get('email', ''),  # Added email field
                        'id': member_info.get('id', '')
                    })
            
            return members_without_goals, members_without_checkins, members_with_goals_no_checkins
            
        except Exception as e:
            st.error(f"Error analyzing missing goals and checkins: {e}")
            return [], [], []

    def load_and_process_data(self, progress_callback=None):
        """Main function to load and process all data"""
        try:
            if progress_callback:
                progress_callback("Loading filtered members...", 0.05)
            
            # Load filtered members first
            filtered_members = self.get_filtered_members()
            if filtered_members.empty:
                st.error("Failed to load filtered members data")
                return None

            if progress_callback:
                progress_callback("Loading users...", 0.1)
            
            # Load users
            users_df = self.get_account_users()
            if users_df.empty:
                st.error("Failed to load users data")
                return None
            
            user_id_to_name = dict(zip(users_df['id'], users_df['name']))

            if progress_callback:
                progress_callback("Loading goals...", 0.2)
            
            # Load Goals
            goals_df = self.get_goals_data()
            if goals_df.empty:
                st.error("Failed to load goals data")
                return None

            if progress_callback:
                progress_callback("Loading KRs...", 0.4)
            
            # Load KRs
            krs_df = self.get_krs_data()

            if progress_callback:
                progress_callback("Loading checkins...", 0.6)
            
            # Load Checkins
            all_checkins = self.get_all_checkins()
            checkin_df = self.extract_checkin_data(all_checkins)

            if progress_callback:
                progress_callback("Processing data...", 0.8)

            # Join all data
            joined_df = goals_df.merge(krs_df, on='goal_id', how='left')
            joined_df['goal_user_name'] = joined_df['goal_user_id'].map(user_id_to_name)
            self.final_df = joined_df.merge(checkin_df, on='kr_id', how='left')

            # Clean data
            self._clean_final_data()

            if progress_callback:
                progress_callback("Data processing completed!", 1.0)

            return self.final_df

        except Exception as e:
            st.error(f"Error in data processing: {e}")
            return None

    def _clean_final_data(self):
        """Clean and prepare final dataset"""
        try:
            # Fill NaN values
            self.final_df['kr_current_value'] = pd.to_numeric(self.final_df['kr_current_value'], errors='coerce').fillna(0.00)
            self.final_df['checkin_kr_current_value'] = pd.to_numeric(self.final_df['checkin_kr_current_value'], errors='coerce').fillna(0.00)

            # Fill dates
            self.final_df['kr_since'] = self.final_df['kr_since'].fillna(self.final_df['goal_since'])
            self.final_df['checkin_since'] = self.final_df['checkin_since'].fillna(self.final_df['kr_since'])

            # Drop unused columns
            columns_to_drop = ['goal_user_id', 'kr_user_id']
            existing_columns_to_drop = [col for col in columns_to_drop if col in self.final_df.columns]
            if existing_columns_to_drop:
                self.final_df = self.final_df.drop(columns=existing_columns_to_drop)

        except Exception as e:
            st.error(f"Error cleaning data: {e}")

    def calculate_current_value(self, df: pd.DataFrame = None) -> float:
        """Calculate current OKR value using average of goal_current_value for unique goal_names"""
        if df is None:
            df = self.final_df
    
        try:
            # Get unique goal_names and their goal_current_value
            unique_goals = df.groupby('goal_name')['goal_current_value'].first().reset_index()
            
            # Convert goal_current_value to numeric
            unique_goals['goal_current_value'] = pd.to_numeric(unique_goals['goal_current_value'], errors='coerce').fillna(0)
            
            # Calculate average of goal_current_value for unique goals
            return unique_goals['goal_current_value'].mean() if len(unique_goals) > 0 else 0
    
        except Exception as e:
            st.error(f"Error calculating current value: {e}")
            return 0

    def calculate_last_friday_value(self, last_friday: datetime, df: pd.DataFrame = None) -> Tuple[float, List[Dict]]:
        """Calculate OKR value as of last Friday"""
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

    def calculate_kr_shift_last_friday(self, row: pd.Series, last_friday: datetime) -> float:
        """Calculate kr_shift_last_friday = kr_current_value - last_friday_checkin_value
        Always compares against Friday of previous week"""
        try:
            # Get current kr value
            kr_current_value = pd.to_numeric(row.get('kr_current_value', 0), errors='coerce')
            if pd.isna(kr_current_value):
                kr_current_value = 0
            
            # Calculate last friday checkin value for this KR
            kr_id = row.get('kr_id', '')
            if not kr_id:
                return kr_current_value  # If no KR ID, shift = current value
            
            # Filter data for this specific KR and find checkins within range
            quarter_start = self.get_quarter_start_date()
            
            # Important: Use the reference Friday (previous Friday) as the cutoff
            reference_friday = self.get_last_friday_date()
            
            kr_checkins = self.final_df[
                (self.final_df['kr_id'] == kr_id) & 
                (self.final_df['checkin_id'].notna()) &
                (self.final_df['checkin_name'].notna()) &
                (self.final_df['checkin_name'] != '')
            ].copy()
            
            # Convert checkin dates and filter by time range up to reference Friday
            if not kr_checkins.empty:
                kr_checkins['checkin_since_dt'] = pd.to_datetime(kr_checkins['checkin_since'], errors='coerce')
                kr_checkins = kr_checkins[
                    (kr_checkins['checkin_since_dt'] >= quarter_start) &
                    (kr_checkins['checkin_since_dt'] <= reference_friday)
                ]
                
                # Get latest checkin value in range (up to reference Friday)
                if not kr_checkins.empty:
                    latest_checkin = kr_checkins.loc[kr_checkins['checkin_since_dt'].idxmax()]
                    last_friday_checkin_value = pd.to_numeric(latest_checkin.get('checkin_kr_current_value', 0), errors='coerce')
                    if pd.isna(last_friday_checkin_value):
                        last_friday_checkin_value = 0
                else:
                    last_friday_checkin_value = 0
            else:
                last_friday_checkin_value = 0
            
            # Calculate shift: current value - value as of reference Friday
            kr_shift = kr_current_value - last_friday_checkin_value
            return kr_shift
            
        except Exception as e:
            st.warning(f"Error calculating kr_shift_last_friday: {e}")
            return 0
    
    def calculate_final_okr_goal_shift(self, user_df: pd.DataFrame) -> float:
        """
        Calculate final_okr_goal_shift using reference to previous Friday:
        1. Group by unique combination of goal_name + kr_name
        2. Calculate average kr_shift_last_friday for each combination
        3. Calculate average of all combination averages
        Always uses Friday of previous week as reference point
        """
        try:
            # Get reference Friday (previous Friday)
            reference_friday = self.get_last_friday_date()
            
            # Create unique combinations mapping
            unique_combinations = {}
            
            # Process each row to calculate kr_shift_last_friday
            for idx, row in user_df.iterrows():
                goal_name = row.get('goal_name', '')
                kr_name = row.get('kr_name', '')
                
                # Skip rows without goal_name or kr_name
                if not goal_name or not kr_name:
                    continue
                
                # Create unique combination key
                combo_key = f"{goal_name}|{kr_name}"
                
                # Calculate kr_shift using reference Friday
                kr_shift = self.calculate_kr_shift_last_friday(row, reference_friday)
                
                # Add to combinations
                if combo_key not in unique_combinations:
                    unique_combinations[combo_key] = []
                unique_combinations[combo_key].append(kr_shift)
            
            # Calculate final_okr_friday_shift for each unique combination
            final_okr_friday_shifts = []
            
            for combo_key, kr_shifts in unique_combinations.items():
                # Calculate average kr_shift_last_friday for this combination
                if kr_shifts:
                    avg_kr_shift = sum(kr_shifts) / len(kr_shifts)
                    final_okr_friday_shifts.append(avg_kr_shift)
            
            # Calculate final_okr_goal_shift (average of all final_okr_friday_shift)
            if final_okr_friday_shifts:
                final_okr_goal_shift = sum(final_okr_friday_shifts) / len(final_okr_friday_shifts)
            else:
                final_okr_goal_shift = 0
            
            return final_okr_goal_shift
            
        except Exception as e:
            st.error(f"Error calculating final_okr_goal_shift: {e}")
            return 0
        
    def calculate_okr_shifts_by_user(self) -> List[Dict]:
        """Calculate OKR shifts for each user always comparing against previous Friday
        If shift > current_value, then shift = current_value - last_friday_value"""
        try:
            users = self.final_df['goal_user_name'].dropna().unique()
            user_okr_shifts = []
    
            # Get reference Friday for all calculations
            reference_friday = self.get_last_friday_date()
    
            for user in users:
                user_df = self.final_df[self.final_df['goal_user_name'] == user].copy()
                
                # Calculate final_okr_goal_shift using reference Friday
                final_okr_goal_shift = self.calculate_final_okr_goal_shift(user_df)
                
                # Calculate current and last Friday values for comparison/legacy
                current_value = self.calculate_current_value(user_df)
                last_friday_value, kr_details = self.calculate_last_friday_value(reference_friday, user_df)
                legacy_okr_shift = current_value - last_friday_value
    
                # NEW LOGIC: If shift > current_value, then shift = current_value - last_friday_value
                adjusted_okr_shift = final_okr_goal_shift
                adjustment_applied = False
                
                if final_okr_goal_shift > current_value:
                    adjusted_okr_shift = current_value - last_friday_value
                    adjustment_applied = True
    
                user_okr_shifts.append({
                    'user_name': user,
                    'okr_shift': adjusted_okr_shift,  # Use adjusted value
                    'original_shift': final_okr_goal_shift,  # Keep original for reference
                    'current_value': current_value,
                    'last_friday_value': last_friday_value,
                    'legacy_okr_shift': legacy_okr_shift,  # Keep old method for reference
                    'adjustment_applied': adjustment_applied,  # Flag to show if adjustment was applied
                    'kr_details_count': len(kr_details),
                    'reference_friday': reference_friday.strftime('%d/%m/%Y')  # Add reference date
                })
    
            # Sort by adjusted okr_shift descending
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
        
        # Calculate last week date range
        today = datetime.now()
        days_since_monday = today.weekday()
        monday_this_week = today - timedelta(days=days_since_monday)
        monday_last_week = monday_this_week - timedelta(days=7)
        sunday_last_week = monday_last_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
        # Calculate weeks in quarter for frequency calculation
        quarter_start = self.get_quarter_start_date()
        weeks_in_quarter = (today - quarter_start).days / 7
        # Ensure we have at least 1 week to avoid division by zero
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
    
                # NEW CALCULATION: Frequency = Total checkins / Weeks passed in quarter
                checkin_frequency = user_total_checkins / weeks_in_quarter
                
                # Calculate checkins in last week
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
                    'checkin_frequency_per_week': checkin_frequency,  # Updated calculation
                    'last_week_checkins': user_last_week_checkins,
                    'weeks_in_quarter': weeks_in_quarter  # Add for reference
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
            color_names = ['success', 'danger', 'info', 'warning', 'purple']
            
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
                    status = 'positive'
                elif value < 0:
                    color = '#E74C3C'
                    bg_color = 'rgba(231, 76, 60, 0.1)'
                    icon = '📉'
                    status = 'negative'
                else:
                    color = '#F39C12'
                    bg_color = 'rgba(243, 156, 18, 0.1)'
                    icon = '➡️'
                    status = 'neutral'
                
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

    def _generate_top_overall_table_html(self, overall_checkins_data):
        """Generate HTML table for top overall checkin users"""
        if not overall_checkins_data:
            return "<div style='text-align: center; padding: 20px; background: #f8f9fa; border-radius: 10px; color: #7f8c8d;'><p>📭 Không có dữ liệu</p></div>"
        
        # Calculate quarter information
        today = datetime.now()
        quarter_start = datetime(today.year, ((today.month - 1) // 3) * 3 + 1, 1)
        weeks_in_quarter = (today - quarter_start).days / 7
        weeks_in_quarter = max(weeks_in_quarter, 1)
        
        # Calculate last week date range
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
            
            # Style for high performers
            row_style = ""
            if i < 3:  # Top 3
                row_style = "style='background: linear-gradient(135deg, #fff9e6, #fffbf0); font-weight: 600;'"
            elif i % 2 == 0:
                row_style = "style='background: #f8f9fa;'"
            
            # Highlight high activity
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
        
        # Add summary statistics
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
                               members_with_goals_no_checkins, okr_shifts):
            """Create HTML email content with fallback charts including top_overall table"""
            
            current_date = datetime.now().strftime("%d/%m/%Y")
            total_members = len(analyzer.filtered_members_df) if analyzer.filtered_members_df is not None else 0
            
            # Calculate statistics
            members_with_goals = total_members - len(members_without_goals)
            members_with_checkins = total_members - len(members_without_checkins)
            
            progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0]) if okr_shifts else 0
            stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0]) if okr_shifts else 0
            issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0]) if okr_shifts else 0
            
            # Get checkin behavior analysis data for top_overall table
            period_checkins, overall_checkins = analyzer.analyze_checkin_behavior()
            
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
            
            # NEW: Generate top_overall table
            top_overall_table = self._generate_top_overall_table_html(overall_checkins[:20] if overall_checkins else [])
            
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
                
                <div class="section">
                    <h2>🏆 TOP NHÂN VIÊN HOẠT ĐỘNG CHECKIN NHIỀU NHẤT</h2>
                    <div class="alert alert-info">
                        <strong>Thống kê:</strong> Xếp hạng dựa trên tổng số checkin và tần suất checkin từ đầu quý
                    </div>
                    {top_overall_table}
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
                    <p>📊 Báo cáo được tạo tự động bởi hệ thống OKR Analysis</p>
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

    def send_email_with_pdf_report(self, email_from, password, email_to, subject, html_content, 
                                  pdf_buffer, company_name="A Plus Mineral Material Corporation"):
        """Send email report with PDF attachment"""
        try:
            # Create message
            message = MIMEMultipart('mixed')  # Changed to 'mixed' for attachments
            message['From'] = f"OKR System {company_name} <{email_from}>"
            message['To'] = email_to
            message['Subject'] = subject
            
            # Create message container for HTML
            msg_alternative = MIMEMultipart('alternative')
            
            # Add HTML content
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg_alternative.attach(html_part)
            
            # Attach HTML part to main message
            message.attach(msg_alternative)
            
            # Add PDF attachment
            if pdf_buffer:
                pdf_attachment = MIMEBase('application', 'pdf')
                pdf_attachment.set_payload(pdf_buffer.getvalue())
                encoders.encode_base64(pdf_attachment)
                
                # Generate filename with current date
                pdf_filename = f"OKR_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                pdf_attachment.add_header(
                    'Content-Disposition',
                    f'attachment; filename="{pdf_filename}"'
                )
                message.attach(pdf_attachment)
            
            # Connect to SMTP server
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(email_from, password)
            
            # Send email
            server.send_message(message)
            server.quit()
            
            return True, "Email with PDF report sent successfully!"
            
        except smtplib.SMTPAuthenticationError:
            return False, "Lỗi xác thực: Vui lòng kiểm tra lại email và mật khẩu"
        except Exception as e:
            return False, f"Lỗi gửi email: {str(e)}"




# ==================== STREAMLIT APP ====================

def main():
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
            index=0,  # Default to first (latest) cycle
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

    run_analysis(analyzer, selected_cycle, show_missing_analysis)

    # Send email report
    if email_button:
        send_email_report_with_pdf(analyzer, email_generator, selected_cycle, email_from, email_password, email_to)

def send_email_report(analyzer, email_generator, selected_cycle, email_from, email_password, email_to):
    """Send email report with analysis results"""
    
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
        
        update_progress("Analyzing missing goals and checkins...", 0.4)
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        
        update_progress("Calculating OKR shifts...", 0.6)
        okr_shifts = analyzer.calculate_okr_shifts_by_user()
        
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

def send_email_report_with_pdf(analyzer, email_generator, selected_cycle, email_from, email_password, email_to):
    """Send email report with PDF attachment"""
    
    st.header("📧 Sending Email Report with PDF")
    
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
        
        update_progress("Analyzing missing goals and checkins...", 0.3)
        members_without_goals, members_without_checkins, members_with_goals_no_checkins = analyzer.analyze_missing_goals_and_checkins()
        
        update_progress("Calculating OKR shifts...", 0.5)
        okr_shifts = analyzer.calculate_okr_shifts_by_user()
        
        update_progress("Creating PDF report...", 0.7)
        # Create PDF report
        pdf_generator = PDFReportGenerator()
        pdf_buffer = pdf_generator.create_pdf_report(
            analyzer, selected_cycle, members_without_goals, members_without_checkins,
            members_with_goals_no_checkins, okr_shifts
        )
        
        update_progress("Creating email content...", 0.8)
        html_content = email_generator.create_email_content(
            analyzer, selected_cycle, members_without_goals, members_without_checkins,
            members_with_goals_no_checkins, okr_shifts
        )
        
        update_progress("Sending email with PDF attachment...", 0.9)
        subject = f"📊 Báo cáo tiến độ OKR & Checkin - {selected_cycle['name']} - {datetime.now().strftime('%d/%m/%Y')}"
        
        # Use the existing email generator with PDF capability (FIXED LINE)
        success, message = email_generator.send_email_with_pdf_report(
            email_from, email_password, email_to, subject, html_content, pdf_buffer
        )
        
        progress_bar.empty()
        status_text.empty()
        
        if success:
            st.success(f"✅ {message}")
            st.info(f"📧 Email report with PDF attachment sent to: {email_to}")
            
            # Show email preview and PDF download option
            col1, col2 = st.columns(2)
            
            with col1:
                if st.checkbox("📋 Show email preview", value=False):
                    st.subheader("Email Preview")
                    st.components.v1.html(html_content, height=600, scrolling=True)
            
            with col2:
                if pdf_buffer:
                    pdf_filename = f"OKR_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                    st.download_button(
                        label="📥 Download PDF Report",
                        data=pdf_buffer.getvalue(),
                        file_name=pdf_filename,
                        mime="application/pdf",
                        key="download_pdf_report"
                    )
        else:
            st.error(f"❌ {message}")
            
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"❌ Error sending email report: {e}")

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
        
        # Calculate OKR shifts
        st.subheader("🎯 OKR Shift Analysis")
        with st.spinner("Calculating OKR shifts..."):
            okr_shifts = analyzer.calculate_okr_shifts_by_user()
        
        if okr_shifts:
            show_okr_analysis(okr_shifts, analyzer.get_last_friday_date())
        else:
            st.warning("No OKR shift data available")
        
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
        show_export_options(df, okr_shifts, period_checkins, overall_checkins, analyzer)
        
        st.success("✅ Analysis completed successfully!")
        
    except Exception as e:
        st.error(f"❌ Analysis failed: {e}")
        progress_bar.empty()
        status_text.empty()

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
        
        # Members Without Goals table below the goals chart
        st.subheader("🚫 Members Without Goals")
        if members_without_goals:
            no_goals_df = pd.DataFrame(members_without_goals)
            st.dataframe(
                no_goals_df[['name', 'username', 'job', 'email']],  # Added email to display
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
        
        # Members with Goals but No Checkins table below the checkins chart
        if members_with_goals_no_checkins:
            st.subheader("⚠️ Members with Goals but No Checkins")
            st.warning("These members have set up goals but haven't made any checkins yet. They may need guidance or reminders.")
            
            goals_no_checkins_df = pd.DataFrame(members_with_goals_no_checkins)
            st.dataframe(
                goals_no_checkins_df[['name', 'username', 'job', 'email']],  # Added email to display
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

# Cập nhật hàm show_okr_analysis để hiển thị ngày tham chiếu
def show_okr_analysis(okr_shifts, last_friday):
    """Show OKR shift analysis with reference date"""
    
    # Display reference information
    st.info(f"📅 **Ngày tham chiếu:** Thứ 6 tuần trước ({last_friday.strftime('%d/%m/%Y')})")
    st.info(f"📊 **Logic tính toán:** So sánh giá trị hiện tại với giá trị tại thứ 6 tuần trước")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    progress_users = len([u for u in okr_shifts if u['okr_shift'] > 0])
    stable_users = len([u for u in okr_shifts if u['okr_shift'] == 0])
    issue_users = len([u for u in okr_shifts if u['okr_shift'] < 0])
    avg_shift = np.mean([u['okr_shift'] for u in okr_shifts])
    
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
        y='okr_shift',
        title=f"Dịch chuyển OKR so với thứ 6 tuần trước ({last_friday.strftime('%d/%m/%Y')})",
        color='okr_shift',
        color_continuous_scale=['red', 'yellow', 'green'],
        labels={
            'user_name': 'Nhân viên',
            'okr_shift': 'Dịch chuyển OKR'
        }
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Top performers table
    st.subheader("🏆 Nhân viên tiến bộ nhất")
    top_performers = okr_df[okr_df['okr_shift'] > 0].head(10)
    if not top_performers.empty:
        display_cols = ['user_name', 'okr_shift', 'current_value', 'last_friday_value']
        display_df = top_performers[display_cols].round(2)
        display_df.columns = ['Nhân viên', 'Dịch chuyển', 'Giá trị hiện tại', f'Giá trị thứ 6 tuần trước']
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("Không có nhân viên nào có dịch chuyển OKR dương")
    
    # Issues table
    if issue_users > 0:
        st.subheader("⚠️ Nhân viên cần hỗ trợ")
        issue_df = okr_df[okr_df['okr_shift'] < 0]
        display_cols = ['user_name', 'okr_shift', 'current_value', 'last_friday_value']
        display_df = issue_df[display_cols].round(2)
        display_df.columns = ['Nhân viên', 'Dịch chuyển', 'Giá trị hiện tại', f'Giá trị thứ 6 tuần trước']
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
    
    # Top checkin users - IMPROVED SECTION with updated frequency calculation
    st.subheader("🏆 Most Active (Overall)")
    
    # Calculate quarter information
    today = datetime.now()
    days_since_monday = today.weekday()
    monday_this_week = today - timedelta(days=days_since_monday)
    monday_last_week = monday_this_week - timedelta(days=7)
    sunday_last_week = monday_last_week + timedelta(days=6)
    
    # Calculate weeks in quarter for context
    weeks_in_quarter = (today - quarter_start).days / 7
    weeks_in_quarter = max(weeks_in_quarter, 1)
    
    st.info(f"📅 Tuần trước: {monday_last_week.strftime('%d/%m/%Y')} - {sunday_last_week.strftime('%d/%m/%Y')}")
    st.info(f"📊 Tần suất checkin = Tổng checkin ÷ {weeks_in_quarter:.1f} tuần (từ đầu quý đến nay)")
    
    # Select and format columns for display
    top_overall = overall_df.nlargest(20, 'total_checkins').copy()
    
    # Create display dataframe with improved formatting
    display_df = top_overall[[
        'user_name', 
        'total_checkins', 
        'checkin_frequency_per_week',
        'last_week_checkins'
    ]].copy()
    
    # Rename columns for better display
    display_df.columns = [
        '👤 Nhân viên',
        '📊 Tổng checkin', 
        '⚡ Tần suất/tuần (quý)',
        '📅 Checkin tuần trước'
    ]
    
    # Round numeric values
    display_df['⚡ Tần suất/tuần (quý)'] = display_df['⚡ Tần suất/tuần (quý)'].round(2)
    
    # Display with improved styling
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
    
    # Add summary metrics for last week activity and quarter frequency
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
    
    # Add frequency distribution chart
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

def show_export_options(df, okr_shifts, period_checkins, overall_checkins, analyzer):
    """Show data export options"""
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
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
        if st.button("🎯 Export OKR Shifts"):
            csv = pd.DataFrame(okr_shifts).to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"okr_shifts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col3:
        if st.button("📝 Export Period Checkins"):
            csv = pd.DataFrame(period_checkins).to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"period_checkins_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col4:
        if st.button("📈 Export Overall Checkins"):
            csv = pd.DataFrame(overall_checkins).to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"overall_checkins_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col5:
        if st.button("👥 Export Filtered Members"):
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

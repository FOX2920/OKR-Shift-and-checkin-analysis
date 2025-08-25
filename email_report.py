"""
Email Report Generator cho OKR Analysis System
"""
import smtplib
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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
        
        # Build complete HTML email content with all sections
        html_content = self._build_complete_html_content(
            selected_cycle, current_date, total_members, members_with_goals, members_with_checkins,
            progress_users, stable_users, issue_users, monthly_stats, okr_shifts_chart, 
            monthly_chart_html, top_overall_table, members_without_goals, goals_table,
            members_with_goals_no_checkins, goals_no_checkins_table, checkins_table,
            top_performers, top_performers_table, top_performers_monthly_table,
            issue_performers, issue_performers_table, issue_performers_monthly_table
        )
        
        return html_content

    def _build_complete_html_content(self, selected_cycle, current_date, total_members, members_with_goals, 
                                   members_with_checkins, progress_users, stable_users, issue_users,
                                   monthly_stats, okr_shifts_chart, monthly_chart_html, top_overall_table,
                                   members_without_goals, goals_table, members_with_goals_no_checkins,
                                   goals_no_checkins_table, checkins_table, top_performers, 
                                   top_performers_table, top_performers_monthly_table, issue_performers,
                                   issue_performers_table, issue_performers_monthly_table):
        """Build complete HTML email content"""
        
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
        if monthly_chart_html:
            current_month = datetime.now().month
            month_name = {2: "Tháng 2", 3: "Tháng 3", 5: "Tháng 5", 6: "Tháng 6", 
                         8: "Tháng 8", 9: "Tháng 9", 11: "Tháng 11", 12: "Tháng 12"}.get(current_month, f"Tháng {current_month}")
            
            html_content += f"""
            <div class="monthly-indicator">
                <strong>🗓️ {month_name}:</strong> Báo cáo này bao gồm phân tích dịch chuyển OKR theo tháng (so với cuối tháng trước)
            </div>
            """
        
        # Continue with all sections
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
        
        # Continue with rest of email content
        html_content += f"""
            <div class="section">
                <h2>🏆 TOP NHÂN VIÊN HOẠT ĐỘNG CHECKIN NHIỀU NHẤT</h2>
                <div class="alert alert-info">
                    <strong>Thống kê:</strong> Xếp hạng dựa trên tổng số checkin và tần suất checkin từ đầu quý
                </div>
                {top_overall_table}
            </div>
        """
        
        # Add remaining sections for goals, top performers, etc.
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
            return False, "❌ Lỗi xác thực email. Vui lòng kiểm tra lại tài khoản và mật khẩu."
        except smtplib.SMTPRecipientsRefused:
            return False, "❌ Địa chỉ email người nhận không hợp lệ."
        except smtplib.SMTPException as e:
            return False, f"❌ Lỗi SMTP: {str(e)}"
        except Exception as e:
            return False, f"❌ Lỗi không xác định: {str(e)}"

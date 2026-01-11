
print("\n" + "="*80)
print("🧪 THỬ NGHIỆM TÍNH ĐIỂM SECTION II (ALIGNMENT, PRIORITY, IMPACT)")
print("="*80)

# Define user-facing columns map 
# App uses english keys internally but goal_test used Vietnamese keys for extraction
col_map = {
    'align': 'Mức độ đóng góp vào mục tiêu công ty',
    'prio': 'Mức độ ưu tiên mục tiêu của Quý',
    'impact': 'Tính khó/tầm ảnh hưởng đến hệ thống'
}

for user_id, user in analyzer.users.items():
    # Get unique goals for this user
    # Check for correct user_id column
    user_col = 'goal_user_id'
    if 'pro_goal_user_id' in analyzer.final_df.columns:
        user_col = 'pro_goal_user_id'
    
    goal_id_col = 'goal_id'
    if 'pro_goal_id' in analyzer.final_df.columns:
        goal_id_col = 'pro_goal_id'
        
    user_goals_df = analyzer.final_df[analyzer.final_df[user_col] == user_id].drop_duplicates(goal_id_col)
    
    if user_goals_df.empty:
        continue
    
    def calculate_median(col_name):
        values = []
        if col_name in user_goals_df.columns:
            for val_str in user_goals_df[col_name]:
                if pd.notna(val_str) and isinstance(val_str, str) and len(val_str) > 0:
                    try:
                        # Take first char (e.g. "1 - ...")
                        first_char = val_str.strip()[0]
                        val = int(first_char)
                        values.append(val)
                    except:
                        pass
        return np.median(values) if values else 1.0

    align_median = calculate_median(col_map['align'])
    prio_median = calculate_median(col_map['prio'])
    impact_median = calculate_median(col_map['impact'])

    print(f"User: {user.name}")
    print(f"  - Goals count: {len(user_goals_df)}")
    print(f"  - Alignment Median: {align_median}")
    print(f"  - Priority Median:  {prio_median}")
    print(f"  - Impact Median:    {impact_median}")
    print("-" * 40)

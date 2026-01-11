import pandas as pd

try:
    df = pd.read_csv('goal_data.csv')
    print("Columns:", list(df.columns))
    # Check for relevant columns
    print("Has 'next_action'?", 'next_action' in df.columns)
    print("Has 'checkin_quality'?", 'checkin_quality' in df.columns)
    print("Has 'form'?", 'form' in df.columns)
except Exception as e:
    print(e)

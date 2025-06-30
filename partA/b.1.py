import pandas as pd

def process_series_time(file_path):
    # קריאת הקובץ
    df = pd.read_csv(file_path)

    print("דוגמא לנתונים:")
    print(df.head())

    # המרת עמודת timestamp לפורמט datetime עם dayfirst=True
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', dayfirst=True)

    # בדיקת תאריכים לא תקינים
    invalid_dates = df['timestamp'].isna().sum()
    print(f"מספר שורות עם תאריך לא תקין: {invalid_dates}")

    # בדיקת כפילויות (שורות זהות)
    duplicates = df.duplicated().sum()
    print(f"מספר שורות כפולות מדויקות: {duplicates}")

    # בדיקת ערכים חסרים בכלל העמודות
    missing_values = df.isna().sum()
    print("ערכים חסרים בעמודות:")
    print(missing_values)

    # המרת עמודת value למספרי (float), טיפול בערכים לא תקינים
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    non_numeric_values = df['value'].isna().sum()
    print(f"מספר ערכים לא מספריים בעמודת value (לאחר המרה): {non_numeric_values}")

    # ניקוי שורות עם תאריך או ערך לא תקין
    df = df.dropna(subset=['timestamp', 'value'])

    # ניקוי כפילויות
    df = df.drop_duplicates()

    # יצירת עמודה חדשה עם השעה העגולה
    df['hour'] = df['timestamp'].dt.floor('h')

    # חישוב ממוצע value לכל שעה
    hourly_avg = df.groupby('hour')['value'].mean().reset_index()

    print("ממוצע ערכים לכל שעה:")
    print(hourly_avg.head())

    return hourly_avg

if __name__ == "__main__":
    file_path = 'time_series.csv'
    hourly_avg_df = process_series_time(file_path)

    hourly_avg_df.to_csv('1', index=False)

import pandas as pd
from datetime import timedelta


def process_chunk(df_chunk):
    df_chunk['value'] = pd.to_numeric(df_chunk['value'], errors='coerce')
    df_chunk = df_chunk.dropna(subset=['timestamp', 'value'])
    df_chunk = df_chunk.drop_duplicates()

    # יצירת עמודת שעה עגולה
    df_chunk['hour'] = df_chunk['timestamp'].dt.floor('h')

    # חישוב ממוצע שעתית בקובץ הקטן
    hourly_avg_chunk = df_chunk.groupby('hour')['value'].mean().reset_index()
    return hourly_avg_chunk


def process_in_chunks(file_path, chunk_days=2):
    # קריאת הקובץ הראשונית (כדי למנוע טעינת כל הקובץ במכה)
    df = pd.read_csv(file_path)

    # המרת timestamp ל-datetime עם dayfirst=True
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', dayfirst=True)

    # מיון לפי timestamp
    df = df.sort_values('timestamp')

    # ניקוי שורות לא תקינות ב timestamp (לפחות)
    df = df.dropna(subset=['timestamp'])

    # הגדרת רשימה לאחסון התוצאות
    all_chunks_results = []

    # מציאת טווח התאריכים
    start_date = df['timestamp'].min().normalize()  # תחילת היום הראשון
    end_date = df['timestamp'].max().normalize()  # תחילת היום האחרון

    current_start = start_date

    # מעגל עד סוף התאריכים בחלונות של chunk_days
    while current_start <= end_date:
        current_end = current_start + timedelta(days=chunk_days)

        # סינון חלק מהדאטה לטווח זמן הנוכחי
        mask = (df['timestamp'] >= current_start) & (df['timestamp'] < current_end)
        df_chunk = df.loc[mask].copy()

        if not df_chunk.empty:
            # עיבוד החלק
            result_chunk = process_chunk(df_chunk)
            all_chunks_results.append(result_chunk)
            print(f"Processed chunk: {current_start.date()} to {current_end.date()}, rows: {len(df_chunk)}")
        else:
            print(f"No data in chunk: {current_start.date()} to {current_end.date()}")

        current_start = current_end

    # איחוד כל התוצאות
    if all_chunks_results:
        combined_df = pd.concat(all_chunks_results)
        # מכיוון שאולי יש חפיפות בשעות בין החלקים (אם לא, זה מיותר)
        # ממוצע נוסף כדי לאחד ערכים באותם השעות (לדוגמה אם היה כפילות)
        final_avg = combined_df.groupby('hour')['value'].mean().reset_index()
    else:
        final_avg = pd.DataFrame(columns=['hour', 'value'])

    return final_avg


if __name__ == "__main__":
    file_path = 'time_series.csv'
    final_result = process_in_chunks(file_path, chunk_days=2)
    print("Final hourly averages:")
    print(final_result.head(20))

    final_result.to_csv('2', index=False)

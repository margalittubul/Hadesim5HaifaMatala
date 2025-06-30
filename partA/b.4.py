import pandas as pd
from datetime import timedelta

def read_series_time(file_path):
    # קורא את הקובץ לפי סוג הקובץ וממיר timestamp.
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        df = pd.read_parquet(file_path)
    else:
        raise ValueError("Unsupported file format. Use .csv or .parquet")

    df.columns = df.columns.str.strip()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', dayfirst=True)
    return df

def detect_value_column(df):
    # מזהה את העמודה המתאימה לערכים במסד הנתונים.
    # מחפש בין האפשרויות הנפוצות.
    possible_cols = ['value', 'mean_value', 'median_value']
    for col in possible_cols:
        if col in df.columns:
            return col
    raise ValueError(f"No known value column found. Tried {possible_cols}")

def process_chunk(df_chunk, value_col):
    # מעבד chunk של נתונים לפי עמודת הערכים שנבחרה.
    df_chunk[value_col] = pd.to_numeric(df_chunk[value_col], errors='coerce')
    df_chunk = df_chunk.dropna(subset=['timestamp', value_col])
    df_chunk = df_chunk.drop_duplicates()

    df_chunk['hour'] = df_chunk['timestamp'].dt.floor('h')

    hourly_avg_chunk = df_chunk.groupby('hour')[value_col].mean().reset_index()
    return hourly_avg_chunk

def process_in_chunks(file_path, chunk_days=2):
 # את הקובץ, מזהה את עמודת הערך,
 #    ומעבד את הנתונים בחלקים לפי ימים,
 #    ואז מאחד את התוצאות.

    df = read_series_time(file_path)

    value_col = detect_value_column(df)
    print(f"Using value column: {value_col}")

    df = df.sort_values('timestamp')
    df = df.dropna(subset=['timestamp'])

    all_chunks_results = []

    start_date = df['timestamp'].min().normalize()
    end_date = df['timestamp'].max().normalize()

    current_start = start_date
    while current_start <= end_date:
        current_end = current_start + timedelta(days=chunk_days)
        mask = (df['timestamp'] >= current_start) & (df['timestamp'] < current_end)
        df_chunk = df.loc[mask].copy()

        if not df_chunk.empty:
            result_chunk = process_chunk(df_chunk, value_col)
            all_chunks_results.append(result_chunk)
            print(f"Processed chunk: {current_start.date()} to {current_end.date()}, rows: {len(df_chunk)}")
        else:
            print(f"No data in chunk: {current_start.date()} to {current_end.date()}")

        current_start = current_end

    if all_chunks_results:
        combined_df = pd.concat(all_chunks_results)
        final_avg = combined_df.groupby('hour')[value_col].mean().reset_index()
    else:
        final_avg = pd.DataFrame(columns=['hour', value_col])

    return final_avg

if __name__ == "__main__":
    file_path = 'time_series.parquet'
    final_result = process_in_chunks(file_path, chunk_days=2)
    print("Final hourly averages:")
    print(final_result.head(20))
    final_result.to_csv('4', index=False)

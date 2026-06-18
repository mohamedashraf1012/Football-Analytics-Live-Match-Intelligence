import pandas as pd

df = pd.read_csv(r"D:\ITI LABS\Grad project\datasets\raw_data\football_events_enriched2.csv")

cols_to_drop = [
    "event_date",
    "club_total_market_value", 
    "competition_id",
]

df.drop(columns=cols_to_drop, inplace=True)

print(f"Columns remaining: {len(df.columns)}")
print(df.columns.tolist())

df.to_csv(r"D:\ITI LABS\Grad project\datasets\raw_data\football_events_clean.csv", index=False)
print("✅ Saved!")
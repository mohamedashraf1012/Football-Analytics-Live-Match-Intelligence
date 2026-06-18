import pandas as pd
import numpy as np

# ==========================
# Load Dataset
# ==========================
file_path = r"D:\ITI LABS\Grad project\datasets\raw_data\football_events_enriched.csv"

df = pd.read_csv(file_path)

print("=" * 80)
print("DATA QUALITY REPORT")
print("=" * 80)

# ==========================
# Basic Information
# ==========================
print("\n📊 Dataset Shape")
print(f"Rows    : {df.shape[0]:,}")
print(f"Columns : {df.shape[1]:,}")

print("\n📋 Data Types")
print(df.dtypes)

# ==========================
# Missing Values
# ==========================
print("\n🔍 Missing Values")

missing = pd.DataFrame({
    "Missing Count": df.isnull().sum(),
    "Missing %": round(df.isnull().mean() * 100, 2)
})

missing = missing.sort_values("Missing %", ascending=False)

print(missing[missing["Missing Count"] > 0])

# ==========================
# Duplicate Rows
# ==========================
duplicates = df.duplicated().sum()

print("\n📌 Duplicate Rows")
print(f"Duplicates: {duplicates:,}")

# ==========================
# Unique Values
# ==========================
print("\n🔢 Unique Values Per Column")

unique_df = pd.DataFrame({
    "Unique Values": df.nunique()
})

print(unique_df.sort_values("Unique Values"))

# ==========================
# Numerical Statistics
# ==========================
print("\n📈 Numerical Summary")

print(df.describe().T)

# ==========================
# Outlier Detection (IQR)
# ==========================
print("\n🚨 Outlier Analysis (IQR Method)")

numeric_cols = df.select_dtypes(include=np.number).columns

outlier_report = []

for col in numeric_cols:

    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)

    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    outliers = df[(df[col] < lower) | (df[col] > upper)]

    outlier_report.append([
        col,
        len(outliers),
        round(len(outliers) / len(df) * 100, 2)
    ])

outlier_df = pd.DataFrame(
    outlier_report,
    columns=["Column", "Outliers", "Outlier %"]
)

print(outlier_df.sort_values("Outliers", ascending=False))

# ==========================
# Constant Columns
# ==========================
constant_cols = [
    col for col in df.columns
    if df[col].nunique(dropna=False) == 1
]

print("\n⚠️ Constant Columns")
print(constant_cols if constant_cols else "None")

# ==========================
# High Missing Columns
# ==========================
print("\n⚠️ Columns with Missing > 50%")

high_missing = missing[missing["Missing %"] > 50]

if len(high_missing):
    print(high_missing)
else:
    print("None")

# ==========================
# Save Reports
# ==========================
missing.to_csv("missing_values_report.csv")
outlier_df.to_csv("outlier_report.csv", index=False)

print("\n✅ Reports Saved:")
print(" - missing_values_report.csv")
print(" - outlier_report.csv")

print("\n🎯 Data Quality Check Completed Successfully")
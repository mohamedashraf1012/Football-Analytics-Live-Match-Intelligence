import pandas as pd

file_path = r"D:\ITI LABS\Grad project\datasets\raw_data\game_events.csv"

print("🔄 جاري فحص الملف والتأكد من سلامة البيانات...")

try:
    # نقرأ أول 5 سطور بس عشان نشوف الهيدر والداتا
    df_head = pd.read_csv(file_path, nrows=5)
    print("\n📊 أول 5 سطور في الملف (الـ Head):")
    print("-" * 60)
    print(df_head)
    print("-" * 60)
    
    # نتأكد من عدد السطور الإجمالي في الملف عشان نعرف نزل كامل ولا لأ
    print("\n⏳ جاري حساب عدد السطور الإجمالي (قد يستغرق ثواني)...")
    with open(file_path, 'r', encoding='utf-8') as f:
        row_count = sum(1 for line in f)
    
    print(f"✅ الفايل سليم تماماً!")
    print(f"🔢 إجمالي عدد السطور جوه الملف: {row_count:,} سطر.")

except Exception as e:
    print(f"❌ الفايل فيه مشكلة فعلاً أو مكسور. الخطأ: {e}")
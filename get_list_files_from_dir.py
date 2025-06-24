import os
import fitz  # PyMuPDF
import re
import mysql.connector
from datetime import datetime
from collections import defaultdict
import pandas as pd

folder_path = "c:/Users/obedenok/geoshtorm/"

# Регулярка для чтения значений
def read_pdf(file_path):
    with fitz.open(folder_path + file_path) as doc:
        text = "\n".join(page.get_text() for page in doc)

    num = r"[-+]?\d+(?:\.\d+)?"

    pattern = re.compile(
        fr"(?P<date>\d{{2}}/\d{{2}})\s+(?P<time>\d{{2}})\s+[^\w\s]?\s*"
        fr"(?P<Dir>{num})\s*"
        fr"(?P<Ws10m>{num})\s*"
        fr"(?P<Wg10m>{num})\s*"
        fr"(?P<Ws50m>{num})\s*"
        fr"(?P<Wg50m>{num})\s*"
        fr"(?P<Ws100m>{num})\s*"
        fr"(?P<Wg100m>{num})\s*"
        fr"(?P<Hs>{num})\s*"
        fr"(?P<Hmax>{num})\s*"
        fr"(?P<Tp>{num})\s*"
        fr"(?P<Tz>{num})\s*"
        fr"(?P<H>{num})\s*"
        fr"(?P<T>{num})\s*"
        fr"(?P<Dir2>{num})\s*"
        fr"(?P<H2>{num})\s*"
        fr"(?P<T2>{num})\s*"
        fr"(?P<Dir3>{num})\s*"
        fr"(?P<Prec>{num})\s*"
        fr"(?P<T2m>{num})\s*"
        fr"(?P<Vis>{num})\s*"
        fr"(?P<Dew_Point>-?\d+(?:\.\d+)?)\s*"
        fr"(?P<Cloud_Cover>\d+)(?=\s|$)"
    )
    return pattern.findall(text)

dataframes = {}

# Список файлов
files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

print('Количество файлов = ',len(files))

# Колонки таблицы
columns = [
    "date", "time", "Dir", "Ws10m", "Wg10m", "Ws50m", "Wg50m", "Ws100m", "Wg100m",
    "Hs", "Hmax", "Tp", "Tz", "H", "T", "Dir2", "H2", "T2", "Dir3", "Prec", "T2m",
    "Vis", "Dew_Point", "Cloud_Cover"
]

for file_path in files:
    file_line = read_pdf(file_path)
    if not file_line:
        continue

    date, hour = file_line[0][0], file_line[0][1]
    #base_date = date.replace("/", "")

    if hour in ['03','04', '05', '06']:
        # Преобразуем в формат '2025-01-23 06:00:00'
        datetime_str = f"2025-{date[3:]}-{date[:2]} {hour}:00:00"
        report_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        table_name = f"{date}_06"
        #####

        df = pd.DataFrame(file_line, columns=columns)

        #####
        # Добавляем DataFrame в словарь
        if table_name not in dataframes:
            dataframes[table_name] = df
        else:
            print(f'Ошибка с {table_name}')

    elif hour in ['16', '17', '18', '19']:
        # Преобразуем в формат '2025-01-23 06:00:00'
        datetime_str = f"2025-{date[3:]}-{date[:2]} {hour}:00:00"
        report_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        table_name = f"{date}_18"
        ######

        df = pd.DataFrame(file_line, columns=columns)

        ######
        # Добавляем DataFrame в словарь
        if table_name not in dataframes:
            dataframes[table_name] = df
        else:
            print(f'Ошибка с {table_name}')

    else:
        table_name = f"{date+'/2025'}_unknown"

    print(f"\nСоздаём таблицу: {table_name} и загружаем данные...")

print(dataframes.keys())


# Целевые часы, которые нужно сохранить
target_hours = [0, 6, 12, 18]

# Здесь будут отфильтрованные таблицы
filtered_dataframes = {}

for table_name, df in dataframes.items():
    df["hour"] = df["time"].astype(int)
    df["full_date"] = df["date"] + "_2025"

    grouped = df.groupby("full_date")
    final_rows = []

    for date, group in grouped:
        for target in target_hours:
            group["abs_diff"] = (group["hour"] - target).abs()
            close_enough = group[group["abs_diff"] <= 2]  # Только в пределах ±2 часов

            if not close_enough.empty:
                nearest = close_enough.loc[close_enough["abs_diff"].idxmin()].copy()
                nearest["time"] = f"{target:02}"  # Приводим к целевому часу
                final_rows.append(nearest)
            # Иначе — ничего не добавляем (нет подходящих строк)

    final_df = pd.DataFrame(final_rows).drop(columns=["abs_diff", "hour", "full_date"])
    filtered_dataframes[table_name] = final_df

# Пример: вывести одну таблицу после фильтрации
for name, df in filtered_dataframes.items():
    #print(f"\n{name}")
    df.sort_index(inplace=True)
    #print(df)

with pd.ExcelWriter("report_tables_18.xlsx") as writer:
    for name, df in filtered_dataframes.items():
        if name.endswith("_06"):
            continue  # Пропускаем таблицы с 06 в конце
        safe_name = name.replace("/", "_")[:31]
        df.to_excel(writer, sheet_name=safe_name, index=False)

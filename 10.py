import fitz  # PyMuPDF
import re
import pandas as pd

# Путь к PDF-файлу
file_path = "StormGeo_Baltyk_II__III_2025012917 (1).pdf"

# 1. Извлечение текста из PDF
with fitz.open(file_path) as doc:
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

matches = pattern.findall(text)
print(len(matches), matches)
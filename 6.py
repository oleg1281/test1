import xarray as xr
import pandas as pd

# Открываем файл
ds = xr.open_dataset("delete/tesstt/predictions10042025_20_30_new.nc", decode_times=True)

# Получаем координаты
time_vals = ds["time"].values
datetime_vals = ds["datetime"].values

# Выводим значения
print("🕒 Координата 'time':")
print(time_vals)

print("\n📅 Координата 'datetime':")
print(datetime_vals)

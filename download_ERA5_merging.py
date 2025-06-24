import xarray as xr
import dask

year = 2021

# Загружаем файлы
surface1 = xr.open_dataset(f"BAZA_GRAPHCAST/1.0_13/era5_surface_1_{year}.nc", decode_timedelta=True)
surface2 = xr.open_dataset(f"BAZA_GRAPHCAST/1.0_13/era5_surface_2_{year}.nc", decode_timedelta=True)
pressure  = xr.open_dataset(f"BAZA_GRAPHCAST/1.0_13/era5_pressure_1_{year}.nc", decode_timedelta=True)

# Преобразуем переменные с фиксированными измерениями (если они есть)
#for ds in [surface1, surface2, pressure]:
#    for var in ["geopotential_at_surface", "land_sea_mask"]:
#        if var in ds:
#            ds[var] = ds[var].isel(batch=0, time=0)

# Объединяем в один Dataset
ds_merged = xr.merge([surface1, surface2, pressure], compat="override")

# Удаляем лишние служебные переменные, если есть
ds_merged = ds_merged.drop_vars(    ["number", "expver"], errors="ignore")

# Сохраняем в файл
output_file = f"BAZA_GRAPHCAST/1.0_13/era5_merged_{year}.nc"
ds_merged.to_netcdf(output_file, format="NETCDF4_CLASSIC")

print(f"✅ Объединение завершено. Сохранено в файл: {output_file}")

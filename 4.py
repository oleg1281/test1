import xarray as xr

# Загружаем файлы
surface1 = xr.open_dataset(f"delete/era5_surface_part1.nc", decode_timedelta=True)
surface2 = xr.open_dataset(f"delete/era5_surface_part2.nc", decode_timedelta=True)
pressure  = xr.open_dataset(f"delete/era5_pressure_part1.nc", decode_timedelta=True)

# Преобразуем переменные с фиксированными измерениями (если они есть)
#for ds in [surface1, surface2, pressure]:
#    for var in ["geopotential_at_surface", "land_sea_mask"]:
#        if var in ds:
#            ds[var] = ds[var].isel(batch=0, time=0)

# Объединяем в один Dataset
ds_merged = xr.merge([surface1, surface2, pressure], compat="override")

# Удаляем лишние служебные переменные, если есть
ds_merged = ds_merged.drop_vars(["number", "expver", "toa_incident_solar_radiation"], errors="ignore")  #  , "toa_incident_solar_radiation"

# Сохраняем в файл
output_file = f"delete/out_file.nc"
ds_merged.to_netcdf(output_file, format="NETCDF3_CLASSIC")

print(f"✅ Объединение завершено. Сохранено в файл: {output_file}")

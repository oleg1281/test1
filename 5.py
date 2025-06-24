import xarray as xr
import numpy as np
import pandas as pd

# Загружаем исходный файл
ds = xr.open_dataset("delete/out_file.nc", decode_timedelta=True)

# Получим шаг времени (timedelta)
time_step = ds.time[1].item() - ds.time[0].item()
datetime_step = pd.to_datetime(ds.datetime.values[0, 1]) - pd.to_datetime(ds.datetime.values[0, 0])

# Сколько новых шагов добавим
n_new = 50

# Новые значения времени и даты
new_time = ds.time.values[-1] + time_step * np.arange(1, n_new + 1)
new_datetime = pd.to_datetime(ds.datetime.values[0, -1]) + datetime_step * np.arange(1, n_new + 1)

# Создадим переменные с NaN только для переменных, у которых есть измерение "time"
new_data_vars = {}
for var in ds.data_vars:
    da = ds[var]
    if "time" in da.dims:
        dims = da.dims
        shape = list(da.shape)
        shape[dims.index("time")] = n_new  # заменим размер оси времени

        # координаты, кроме "datetime", иначе будет ошибка при concat
        coords = {k: v for k, v in da.coords.items() if k in dims and k != "datetime"}
        coords["time"] = new_time

        new_da = xr.DataArray(
            data=np.full(shape, np.nan, dtype=da.dtype),
            dims=dims,
            coords=coords
        )

        # Убираем координаты для безопасного слияния
        if "datetime" in da.coords:
            da = da.reset_coords(names="datetime", drop=True)

        # Объединяем
        new_data_vars[var] = xr.concat([da, new_da], dim="time")
    else:
        new_data_vars[var] = da  # без изменений

# Объединяем time и datetime координаты
new_time_full = np.concatenate([ds.time.values, new_time])
new_datetime_full = np.concatenate([ds.datetime.values[0], new_datetime])
datetime_expanded = np.expand_dims(new_datetime_full, axis=0)

# Создаём итоговый Dataset
ds_out = xr.Dataset(new_data_vars)
ds_out = ds_out.assign_coords(time=("time", new_time_full))
ds_out = ds_out.assign_coords(datetime=(("batch", "time"), datetime_expanded))

# если batch была координатой
if "batch" in ds.coords:
    ds_out = ds_out.assign_coords(batch=ds.coords["batch"])

# Сохраняем результат
ds_out.to_netcdf(f"w:/Postprocesing/Oleh Bedenok/GRAPHCAST/NOAA/TEST_Graphcast_14,05,2025-31,05,2025/dataset_NOAA/out_file.nc", format="NETCDF3_CLASSIC")
print("✅ Готово: данные расширены и сохранены в delete/out_file_patch2.nc")

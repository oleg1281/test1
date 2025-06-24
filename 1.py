import cdsapi
import datetime
import numpy as np
import xarray as xr
import cdsapi
import os

c = cdsapi.Client(
    url='https://cds.climate.copernicus.eu/api',
    key='d1c02362-b7bf-453b-9da4-ce9ddad97925'
)

filename = "era5_surface_part1.nc"

def download_surface_part1(filename):
    """
    Скачивает surface-поля ERA5 за указанные datetime (шаг 6 часов),
    затем удаляет лишние временные метки.
    """
    print('📥 Скачивание surface-полей ERA5...')

    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': [
                 'geopoten       tial',
                'land_sea_mask',
                '2m_temperature',
                'mean_sea_level_pressure',
                '10m_v_component_of_wind',
                '10m_u_component_of_wind',
            ],
            'year': '2025',
            'month': '05',
            'day': ['14'],
            'time': ['00:00', '06:00', '12:00', '18:00'],
            'format': 'netcdf',
            'grid': [1.0, 1.0],
        },
        filename
    )

    print(f'✅ Скачивание surface_part1 завершено → {filename}')

def change_part1():
    ds = xr.load_dataset(filename, decode_timedelta=True)

    # Переименовываем координаты
    ds = ds.rename({
        "valid_time": "time",
        "latitude": "lat",
        "longitude": "lon",
        "z": "geopotential_at_surface",
        "lsm": "land_sea_mask",
        "t2m": "2m_temperature",
        "msl": "mean_sea_level_pressure",
        "v10": "10m_v_component_of_wind",
        "u10": "10m_u_component_of_wind",
    })

    # Добавляем ось batch
    ds = ds.expand_dims(dim={"batch": [0]})

    # Преобразуем time в timedelta64[ns]
    base_time = ds.time.values[0]
    time_delta = ds.time.values - base_time
    ds = ds.assign_coords(time=time_delta)

    # Добавляем datetime и делаем его координатой
    datetime = ds.time.values + base_time
    ds["datetime"] = (("batch", "time"), np.expand_dims(datetime, axis=0))
    ds = ds.set_coords("datetime")

    # Удаляем лишние переменные (если есть)
    ds = ds.drop_vars(["number", "expver"], errors="ignore")

    # Меняем порядок осей у переменной
    ds["geopotential_at_surface"] = ds["geopotential_at_surface"].transpose("batch", "time", "lat", "lon")
    ds["land_sea_mask"] = ds["land_sea_mask"].transpose("batch", "time", "lat", "lon")
    ds["2m_temperature"] = ds["2m_temperature"].transpose("batch", "time", "lat", "lon")
    ds["mean_sea_level_pressure"] = ds["mean_sea_level_pressure"].transpose("batch", "time", "lat", "lon")
    ds["10m_v_component_of_wind"] = ds["10m_v_component_of_wind"].transpose("batch", "time", "lat", "lon")
    ds["10m_u_component_of_wind"] = ds["10m_u_component_of_wind"].transpose("batch", "time", "lat", "lon")

    # Сортируем по широте
    ds = ds.sortby("lat")

    # Приводим lat/lon к float32
    ds = ds.assign_coords({
        "lat": ds["lat"].astype(np.float32),
        "lon": ds["lon"].astype(np.float32)
    })

    # Обрезаем до первых 6 временных шагов
    #ds = ds.isel(time=slice(0, 42))

    # Удаляем batch из координат (делаем как в оригинале)
    if "batch" in ds.coords:
        ds = ds.swap_dims({"batch": "batch"})  # сохраняем размерность
        ds = ds.drop_vars("batch")  # убираем как координату

    # Убираем time и batch из geopotential_at_surface и land_sea_mask
    if set(ds["geopotential_at_surface"].dims) == {"batch", "time", "lat", "lon"}:
        ds["geopotential_at_surface"] = ds["geopotential_at_surface"].isel(batch=0, time=0)
    if set(ds["land_sea_mask"].dims) == {"batch", "time", "lat", "lon"}:
        ds["land_sea_mask"] = ds["land_sea_mask"].isel(batch=0, time=0)

    print('Обработка ds_surface_part1 завершена')

    # Сохраняем
    ds.to_netcdf("delete/era5_surface_part1.nc", format="NETCDF4_CLASSIC")


download_surface_part1(filename)
change_part1()
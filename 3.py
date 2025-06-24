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

filename = "era5_pressure_part1.nc"

def download_pressure_part1(filename):
    """
            Скачивает surface-поля ERA5 за указанные datetime (шаг 6 часов)
            """
    print('📥 Скачивание поверхностных данных (pressure-levels_part1)...')

    c.retrieve(
        'reanalysis-era5-pressure-levels',
        {
            'product_type': 'reanalysis',
            'variable': [
                'temperature',
                'geopotential',
                'u_component_of_wind',
                'v_component_of_wind',
                'vertical_velocity',
                'specific_humidity',
            ],
            'pressure_level': [
                '50', '100', '150', '200', '250', '300', '400', '500',
                '600', '700', '850', '925', '1000',
            ],
            'year': '2025',
            'month': '05',
            'day': ['14'],
            'time': ['00:00', '06:00', '12:00', '18:00'],
            'format': 'netcdf',
            'grid': [1.0, 1.0],
            #'area': [90, 0, -90, 359],
        },
        filename
    )
    print(f'✅ Скачивание pressure_part1 завершено → {filename}')

def change_pressure_part1():
    ds = xr.load_dataset(filename, decode_timedelta=True)

    # Переименовываем координаты
    ds = ds.rename({
        "z": "geopotential",
        "w": "vertical_velocity",
        "latitude": "lat",
        "longitude": "lon",
        "valid_time": "time",
        "v": "v_component_of_wind",
        "u": "u_component_of_wind",
        "t": "temperature",
        "q": "specific_humidity",
        "pressure_level": "level"
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
    ds["u_component_of_wind"] = ds["u_component_of_wind"].transpose("batch", "time", "level", "lat", "lon")
    #ds["total_precipitation_6hr"] = ds["total_precipitation_6hr"].transpose("batch", "time", "lat", "lon")

    # Сортируем по широте
    ds = ds.sortby("lat")

    # Oтсортировать уровень давления по возрастанию
    ds = ds.sortby("level")

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

    print('Обработка era5_pressure_part1.nc завершена')

    # Сохраняем
    ds.to_netcdf("delete/era5_pressure_part1.nc", format="NETCDF4_CLASSIC")

download_pressure_part1(filename)
change_pressure_part1()
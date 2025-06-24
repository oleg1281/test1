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

filename = "era5_surface_part2.nc"

def download_surface_part2(filename):
    """
        Скачивает surface-поля ERA5 за указанные datetime (шаг 6 часов)
        """
    print('📥 Скачивание поверхностных данных (single-levels_part2)...')

    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': [
                'total_precipitation',
                'toa_incident_solar_radiation'
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
    print(f'✅ Скачивание surface_part2 завершено → {filename}')


def change_part2():
    ds = xr.load_dataset(filename, decode_timedelta=True)

    # Переименовываем координаты
    ds = ds.rename({
        "valid_time": "time",
        "tp": "total_precipitation_6hr",
        "latitude": "lat",
        "longitude": "lon",
        "tisr": "toa_incident_solar_radiation"
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
    ds["toa_incident_solar_radiation"] = ds["toa_incident_solar_radiation"].transpose("batch", "time", "lat", "lon")
    ds["total_precipitation_6hr"] = ds["total_precipitation_6hr"].transpose("batch", "time", "lat", "lon")

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

    print('Обработка ds_surface_part2 завершена')

    # Сохраняем
    ds.to_netcdf("delete/era5_surface_part2.nc", format="NETCDF4_CLASSIC")

download_surface_part2(filename)
change_part2()
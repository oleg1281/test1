import cdsapi
import xarray as xr
import numpy as np

c = cdsapi.Client()

year = 2016

def download_pressure(filename):
    print('download pressure')
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
            'year': f'{year}',
            'month': '03',
            #'day': ['11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22'],
            'day': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16'],
            'time': ['00:00', '06:00', '12:00', '18:00'],
            'format': 'netcdf',
            'grid': [0.25, 0.25],  # более высокая детализация
            'area': [90, 0, -90, 359],
        },
        filename
    )
    print(f'download {filename} succesfull')


def change(ds, filename):
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

    # Сохраняем
    ds.to_netcdf(filename, format="NETCDF4_CLASSIC")

    print(f"✅ Файл преобразован и сохранён как {filename}")


while year <= 2025:
    filename = f"BAZA_GRAPHCAST/0.25_13/era5_pressure_1_{year}.nc"

    download_pressure(filename)

    ds_surface = xr.load_dataset(filename, decode_timedelta=True)
    change(ds_surface, filename)
    ds_surface.close()

    year += 1

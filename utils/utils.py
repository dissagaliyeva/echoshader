import glob
import xarray as xr
import echopype as ep
import geopandas as gpd
import hvplot.pandas  # noqa


def combine_data(path='data'):
    """
    This function reads in and combines *.nc files
    if they contain latitude/longitude columns.

    Parameters
    ----------
    path : Path to file(s) location
        return: (Default value = 'data')

    Returns
    -------
    Combined Xarray dataset concatenated on datetime column

    """

    paths = glob.glob(f'{path}/*.nc')
    assert len(paths) >= 1, 'No NetCDF files present.'

    temp = []
    for path in paths:
        file = ep.open_converted(path)
        if not isinstance(file.platform, xr.core.dataset.Dataset):
            print(f'Skipping file {path} since it does not have latitude/longitude')
        else:
            temp.append(file)

    return ep.combine_echodata(temp)


def get_geo(df):
    """

    Parameters
    ----------
    df :
        return:

    Returns
    -------

    """
    geo = df.platform.latitude.to_dataframe()\
          .join(df.platform.longitude.to_dataframe())
    return gpd.GeoDataFrame(
            geo,
            geometry=gpd.points_from_xy(geo['longitude'],
                                        geo['latitude']),
            crs=4326)


def compute_mvbs(df, meter=5, ping_time='20s'):
    """

    Parameters
    ----------
    df :
        param meter:
    ping_time :
        return: (Default value = '20s')
    meter :
         (Default value = 5)

    Returns
    -------

    """
    Sv_ds = ep.calibrate.compute_Sv(df)
    return ep.preprocess.compute_MVBS(Sv_ds, range_meter_bin=meter,
                                      ping_time_bin=ping_time)

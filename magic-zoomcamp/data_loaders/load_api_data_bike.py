import io
import pandas as pd
import requests
from zipfile import ZipFile

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    # Decleare an empty dataframe to concat all the dataframes
    full_df = pd.DataFrame()
    year = kwargs['year']

    bike_dtypes = {
            'ride_id': str,
            'rideable_type': str,
            'start_station_name': str,
            'start_station_id': pd.Int64Dtype(),
            'end_station_name': str,
            'end_station_id': pd.Int64Dtype(),
            'start_lat': float,
            'start_lng': float,
            'end_lat': float,
            'end_lng': float,
            'member_casual': str
    }
    parse_dates = ['started_at', 'ended_at']

    for i in range(1,13):
        month_str = f"{i:02d}"
        url = f'https://s3.amazonaws.com/capitalbikeshare-data/{year}{month_str}-capitalbikeshare-tripdata.zip'
        print(url)
        # df = pd.read_csv(url, sep=",", compression="zip", dtype=bike_dtypes, na_values='', parse_dates=parse_dates)

        response = requests.get(url)
        zipfile = ZipFile(io.BytesIO(response.content))
        # Filter out the macOS metadata file and extract the CSV file name
        csv_file = [f for f in zipfile.namelist() if not f.startswith('__MACOSX/')][0]
        
        with zipfile.open(csv_file) as f:
            df = pd.read_csv(f, sep=",", dtype=bike_dtypes, parse_dates=parse_dates)
        full_df = pd.concat([full_df, df], ignore_index=True)
        print(full_df.shape)
    return full_df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

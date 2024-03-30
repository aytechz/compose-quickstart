from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import pyarrow as pa
import pyarrow.parquet as pq


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    year = kwargs['year']

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'capital_bikeshare_mage'

    for i in range(1, 13):
        try:
            # Filter the DataFrame to include only data from the specific month
            # Assuming 'started_at' is the column that contains the date of each ride
            # You need to adjust this to match your actual date column and filtering logic
            month_df = df[df['started_at'].dt.month == i]

            object_key = f'bikeshare-{year}-{i:02d}.parquet'

            GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
                month_df,
                bucket_name,
                object_key,
            )
        except Exception as e:
            print(f"An error occurred while exporting data for month {i}: {e}")
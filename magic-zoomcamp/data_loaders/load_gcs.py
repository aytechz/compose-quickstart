import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Load multiple data files from a Google Cloud Storage bucket based on a naming pattern and range.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    bucket_name = 'capital_bikeshare_mage'
    year = kwargs['year']  # Example year, adjust as necessary
    object_keys = [f'bikeshare-{year}-{i:02d}.parquet' for i in range(1, 13)]  # Generate object keys for each month

    storage = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile))
    
    data_frames = []
    for object_key in object_keys:
        try:
            df = storage.load(bucket_name, object_key)
            data_frames.append(df)
        except Exception as e:
            print(f"An error occurred while loading {object_key}: {e}")

    # Combine all dataframes into a single DataFrame
    combined_df = pd.concat(data_frames, ignore_index=True)

    return combined_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

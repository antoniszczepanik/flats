"""
Pipeline to concatinate all csv's scraped and
deduplicate all csv's scraped from morizon.
It concats data from each folder - sale and rent
and saves them on s3 in 'morizon-data/raw' folder.
"""
from datetime import datetime
import tempfile
import logging as log
import pandas as pd
from s3_bucket import S3Bucket
from utils import concat_csvs

FOLDERS_TO_CONCAT = ['morizon_sale/raw', 'morizon_rent/raw']
BUCKET_NAME = 'morizon-data'
# use if you want to select key-pair from ~/.aws/config, else comment out
S3_PROFLE = 's3-private'

# save logs to a file (later displayed on server, hopefully)
#datetime = datetime.now().strptime()
#log.basicConfig(filename='{s3_folder_name}_cleaning_{datetime}.log',
                    #format='%(name)s - %(levelname)s - %(message)s')
# log = logging.getLogger(__name__)
log.basicConfig(level=log.INFO, format='%(asctime)s %(message)s',
                datefmt='%m-%d-%Y %I:%M:%S')


if __name__ == '__main__':

    log.info('Starting data concatination pipeline')
    with tempfile.TemporaryDirectory() as tmpdir:
        for s3_folder in FOLDERS_TO_CONCAT:
            bucket = S3Bucket(bucket_name=BUCKET_NAME,
                              s3_dir_path=s3_folder,
                              local_downloads_dir=tmpdir,
                              s3_target_dir_path=f'{s3_folder.split("/")[0]}/concated',
                              profile=S3_PROFLE)
            bucket.download_files()
            s3_paths = bucket.list_paths()
            local_paths = [f'{tmpdir}/{path.split("/")[-1]}' for path in s3_paths]
            concatinated_df = concat_csvs(local_paths)

            current_dt = datetime.now().strftime('%Y_%m_%dT%H_%M_%S')
            full_filename = f'{s3_folder.split("/")[0]}_full_{current_dt}.parquet'
            concatinated_df.to_parquet(f'{tmpdir}/{full_filename}', index=False)
            bucket.upload_file(f'{tmpdir}/{full_filename}',
                               full_filename)
            log.info('Succesfully uploaded full file')
    log.info('Finished data_concatination pipeline.')

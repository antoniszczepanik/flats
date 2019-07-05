import pandas as pd
from datetime import datetime
import boto3
import tempfile

FOLDERS_TO_CONCAT = ['morizon_sale', 'morizon_rent']
BUCKET_NAME = 'morizon-data'
SESSION_NAME = 's3-private'

session = boto3.session.Session(profile_name=SESSION_NAME)

class S3bucket(object):

    def __init__(self, bucket_name, s3_dir_path, local_downloads_dir):
        self.bucket_name = bucket_name
        self.s3_dir_path = s3_dir_path
        self.local_downloads_dir = local_downloads_dir
        self.data_bucket = session.resource('s3').Bucket(self.bucket_name)


    def download_files(self):
        filepaths = self.list_files()
        for path in filepaths:
            self.download_file(path)


    def list_files(self, allowed_extension='.csv'):
        raw_file_objects_found = self.data_bucket.objects.filter(Prefix=self.s3_dir_path)
        filepaths = [file.key for file in raw_file_objects_found if file.key.lower().endswith(allowed_extension)]
        return filepaths


    def download_file(self, s3_path):
        file_name = s3_path.split('/')[-1]
        self.data_bucket.download_file(s3_path, f'{self.local_downloads_dir}/{file_name}')

    # upload to the top directory of a bucket, not s3_dir_path
    def upload_file(self, file_name):
        with open(file_name, 'rb') as local_file:
            file_name_no_path = filename.split('/')[0]
            self.data_bucket.put_object(Key=file_name, Body=local_file)


if __name__=='__main__':
    with tempfile.TemporaryDirectory() as tmpdirname:
        for folder in FOLDERS_TO_CONCAT:
            bucket = S3bucket(bucket_name=BUCKET_NAME, s3_dir_path=folder, local_downloads_dir=tmpdirname)
            bucket.download_files()
            filepaths = [f'{tmpdirname}/{filename}' for filename in bucket.list_files()]
            dfs = []
            for filepath in filepaths:
                df = pd.read_csv(filepath)
                dfs.append(df)
            concated_df = pd.concat(dfs).drop_duplicates()
            current_datetime_str = datetime.now().strftime('%Y_%m_%dT%H_%M_%S')
            full_filename = f'{tmpdirname}/{folder}_full_{current_datetime_str}'
            concated_df.to_csv(full_filename)
            bucket.upload_file(full_filename)

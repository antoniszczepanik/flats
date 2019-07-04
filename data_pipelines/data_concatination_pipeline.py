import pandas as pd
from datetime import datetime
import boto3
import tempfile

FOLDERS_TO_CONCAT = ['morizon_sale', 'morizon_rent']
BUCKET_NAME = 'morizon-data'


class S3bucket(object):

    def __init__(self, bucket_name, s3_dir_path, dir_to_download):
        self.bucket_name = bucket_name
        self.s3_dir_path = s3_dir_path
        self.dir_to_download = dir_to_download
        self.data_bucket = boto3.resource('s3').Bucket(self.bucket_name)


    def download_files(self):
        filepaths = self.list_files()
        for path in filepaths:
            self.download_file(path)


    def list_files(self, allowed_extension='.csv'):
        raw_file_objects_found = self.data_bucket.objects.filter(Prefix=self.s3_dir_path)
        filenames = [file.key for file in raw_file_objects_found if file.key.lower().endswith(allowed_extension)]
        return filenames


    def download_file(self, s3_path):
        file_name = s3_path.split('/')[-1]
        self.data_bucket.download_file(s3_path, f'{self.dir_to_download}/{file_name}')

    # upload to the top directory of a bucket, not s3_dir_path
    def upload_file(self, file_name):
        with open(file_name, 'rb') as local_file:
            file_name_no_path = filename.split('/')[0]
            self.data_bucket.put_object(Key=file_name, Body=file_name_no_path)


if __name__=='__main__':
    with tempfile.TemporaryDirectory() as tmpdirname:
        for folder in FOLDERS_TO_CONCAT:
            bucket = S3bucket(bucket_name=BUCKET_NAME, s3_dir_path=folder, dir_to_download=tmpdirname)
            bucket.download_files()
            filelist = [f'{tmpdirname}/{filename}' for filename in bucket.list_files()]
            dfs = []
            for single_file in filelist:
                df = pd.read_csv(single_file)
                dfs.append(df)
            concated_df = pd.concat(dfs).drop_duplicates()
            current_datetime_str = datetime.now().strftime('%Y_%m_%dT%H_%M_%S')
            full_filename = f'{tmpdirname}/{folder}_full_{current_datetime_str}'
            concated_df.to_csv(full_filename)
            bucket.upload_file()

import boto3
import logging as log


class S3Bucket(object):

    def __init__(self,
                 bucket_name,
                 s3_dir_path,
                 local_downloads_dir=None,
                 profile=None,
                 s3_target_dir_path=None):
        if profile:
            session = boto3.session.Session(profile_name=profile)
        self.bucket_name = bucket_name
        self.s3_dir_path = s3_dir_path
        self.s3_target_dir_path = s3_target_dir_path
        self.local_downloads_dir = local_downloads_dir
        self.data_bucket = session.resource('s3').Bucket(self.bucket_name)
        log.info(f'Initialized S3 bucket - {self.bucket_name}')

    # download all files in s3_dir_path
    def download_files(self):
        s3_filepaths = self.list_paths()
        for s3_path in s3_filepaths:
            self.download_file(s3_path)
        return s3_filepaths

    # list filepaths given in s3_dir_path
    def list_paths(self, allowed_extension='.csv'):
        try:
            files = self.data_bucket.objects.filter(Prefix=self.s3_dir_path)
        except Exception as e:
            log.warning(e)
        filepaths = [f.key for f in files if f.key.lower().endswith(allowed_extension)]
        return filepaths

    # download file to self.local_downloads_dir
    def download_file(self, s3_path):
        file_name = s3_path.split('/')[-1]
        log.info(f'Downloading {s3_path} ...')
        if self.local_downloads_dir:
            download_path = f'{self.local_downloads_dir}/{file_name}'
        else:
            download_path = file_name
        try:
            self.data_bucket.download_file(s3_path, download_path)
        except Exception as e:
            log.warning(e)

    # upload to the top directory of a bucket, not s3_dir_path
    def upload_file(self, local_file, target_name):
        if self.s3_target_dir_path:
            target_path = f'{self.s3_target_dir_path}/{target_name}'
        else:
            target_path = target_name
        log.info(f'Uploading {local_file} to {target_path}')
        with open(local_file, 'rb') as f:
            try:
                self.data_bucket.put_object(Key=target_path, Body=f)
            except Exception as e:
                log.warning(e)

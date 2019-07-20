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
import utils

FOLDERS_TO_CONCAT = ["morizon_sale/raw", "morizon_rent/raw"]
BUCKET_NAME = "morizon-data"
ALREADY_CONCATED_FILES_LIST = "{spider_name}_concated_files_list.txt"
S3_TARGET_DIR = "{spider_name}/concated"
# which key-pair s3 value to use
# S3_PROFLE = 's3-private'
S3_PROFILE = "default"

log.basicConfig(
    level=log.INFO, format="%(asctime)s %(message)s", datefmt="%m-%d-%Y %I:%M:%S"
)
if __name__ == "__main__":

    log.info("Starting data concatination pipeline")
    for s3_folder in FOLDERS_TO_CONCAT:
        with tempfile.TemporaryDirectory() as tmpdir:
            spider_name = s3_folder.split("/")[0]
            s3_target_dir = S3_TARGET_DIR.format(spider_name=spider_name)
            concated_files_list_path = ALREADY_CONCATED_FILES_LIST.format(
                spider_name=spider_name
            )

            bucket = S3Bucket(
                bucket_name=BUCKET_NAME,
                s3_dir_path=s3_folder,
                local_downloads_dir=tmpdir,
                s3_target_dir_path=s3_target_dir,
                profile=S3_PROFILE,
            )

            # Find paths different than in concated_files_list
            s3_paths = bucket.list_paths()
            # skip downloading desc files
            s3_paths = [f for f in s3_paths if "desc" not in f]
            try:
                already_concated = utils.read_txt_list(concated_files_list_path)
            except FileNotFoundError:
                already_concated = []
                log.info("Did not find already concated list")
            else:
                log.info("Found already concacted list.")
            paths_to_concat = list(set(s3_paths) - set(already_concated))

            utils.update_txt_list(paths_to_concat, concated_files_list_path)
            log.info(
                f"Updated already concated list with {len(paths_to_concat)} new filepaths."
            )

            for path in paths_to_concat:
                bucket.download_file(path)

            concated_files = bucket.list_paths(
                directory=s3_target_dir, allowed_extension=".parquet"
            )
            # skip downloading desc files
            concated_files = [f for f in concated_files if "desc" not in f]
            previous_concated = utils.select_most_up_to_date_file(concated_files)
            if len(previous_concated) > 0:
                log.info(
                    "Selected most up-to-date file from previously concated: %s",
                    previous_concated,
                )
                bucket.download_file(previous_concated)
            else:
                log.info("Did not find previously concatinated files.")

            local_paths = [
                f"{tmpdir}/{utils.name_from_path(path)}" for path in paths_to_concat
            ]
            local_concated_path = f"{tmpdir}/{utils.name_from_path(previous_concated)}"

            concatinated_df, concatinated_desc = utils.concat_dfs(
                local_paths + [local_concated_path]
            )

            current_dt = datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
            full_filename = f'{s3_folder.split("/")[0]}_full_{current_dt}.parquet'
            full_filename_desc = f'{s3_folder.split("/")[0]}_desc_{current_dt}.parquet'
            concatinated_df.to_parquet(f"{tmpdir}/{full_filename}", index=False)
            if not concatinated_desc.empty:
                concatinated_desc.to_parquet(
                    f"{tmpdir}/{full_filename_desc}", index=False
                )
                bucket.upload_file(
                    f"{tmpdir}/{full_filename_desc}", f"desc/{full_filename_desc}"
                )

            bucket.upload_file(f"{tmpdir}/{full_filename}", full_filename)
            log.info("Succesfully uploaded full file.")
    log.info("Finished data_concatination pipeline.")

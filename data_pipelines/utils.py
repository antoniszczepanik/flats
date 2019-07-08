import pandas as pd
import logging as log

def concat_csvs(csv_paths):
    # concat all df's and drop duplicates
    dfs = []
    total_rows_n = 0
    for path in csv_paths:
        df = pd.read_csv(path)
        total_rows_n += len(df)
        dfs.append(df)
    concatinated_df = pd.concat(dfs, sort=True).drop_duplicates()

    log.info(f'Concatinated {len(dfs)} csv files.')
    log.info(f'Dropped {total_rows_n - len(concatinated_df)} duplicates.')
    log.info(f'Concatinated csv has {len(concatinated_df)} rows.')
    return concatinated_df

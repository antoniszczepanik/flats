from data_concat import concat_csvs_to_parquet

def test_concat_csvs_to_parquet():
    test_in_path = 'test_in_path'
    test_out_path = 'test_out_path'
    test_spider_name = 'test'
    concat_csvs_to_parquet(test_in_path, test_outpath, test_spider_name)
    df = pd.read_parquet()
    assert concat_csvs_to_parquet(test_in_path, test_outpath)

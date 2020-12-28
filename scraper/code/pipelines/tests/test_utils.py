import columns as c
from pipelines import utils

import pandas as pd

def test_name_from_path():
    filepath = 'random1/random2/random3.py'
    assert utils.name_from_path(filepath) == 'random3.py'

def test_closest_point():
    example_point, example_points = (13, 13), [(11, 11), (10,10), (12, 12)]
    assert utils.closest_point(example_point, example_points) == (12, 12)

def test_unzip_coord_series_to_lon_and_lat():
    zipped_colname = 'example'
    df = pd.DataFrame()
    df[zipped_colname] = [(1, 5), (2,6), (3,7), (4,8)]
    output_df = utils.unzip_coord_series_to_lon_and_lat(df, zipped_colname)
    assert list(output_df[c.LAT].values) == [1, 2, 3, 4]
    assert list(output_df[c.LON].values) == [5, 6, 7, 8]
    assert zipped_colname not in output_df.columns

def test_add_zipped_coords_column():
    df = pd.DataFrame()
    df[c.LAT] = [1, 2, 3, 4]
    df[c.LON] = [5, 6, 7, 8]
    new_colname = 'example'
    output = utils.add_zipped_coords_column(df, new_colname)
    assert list(output[new_colname].values) == [(1, 5), (2,6), (3,7), (4,8)]


if __name__ == "__main__":
    test_name_from_path()
    test_closest_point()
    test_unzip_coord_series_to_lon_and_lat()
    test_add_zipped_coords_column()
    print("All passed :)")

#!/usr/bin/env python3 

"""
Based on clean data and coords encoding map add new coords features.
"""
import pandas as pd
from geopy.distance import great_circle

def read_newest_cleaning_map():
    pass

def add_coords_features():
    pass

def get_distance(coords_df):
    """
    Calculate distace between two coords tuples.
    Takes dataframe with coords tuples as an argument.
    """
    distances = []
    for _, row in coords_df.iterrows():
        distances.append(great_circle(row[coords_df.columns[0]],
                                      row[coords_df.columns[1]]).km)
    return [round(dist, 3) for dist in distances]

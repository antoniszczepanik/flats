#!/usr/bin/env python3

import sys
import requests

import pandas as pd

if len(sys.argv) == 1:
    print("Sample usage:")
    print("./upload_test_data.py test_file.csv")

df = pd.read_csv(sys.argv[1])
url = "http://localhost:8000/api/v1/offers/"
headers = {'Content-type': 'application/json'}
count = 0
for ix in df.index:
    row_as_json = df.loc[ix].to_json()
    response = requests.post(url, headers=headers, data=str(row_as_json))
    if count % 500 == 0:
        print(f"Processed {count} offers")
    count+=1

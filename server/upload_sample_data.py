#!/usr/bin/env python3

import sys
import requests

import pandas as pd

if len(sys.argv) == 1:
    print("Sample usage:")
    print("./upload_test_data.py rent test_file.csv")

df = pd.read_csv(sys.argv[2])
df['type__offer'] = sys.argv[1]
url = "http://localhost:5000/offers/"
headers = {'Content-type': 'application/json'}

for ix in df.index:
    row_as_json = df.loc[ix].to_json()
    response = requests.post(url, headers=headers, data=str(row_as_json))
    print(response.text)

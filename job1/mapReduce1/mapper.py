#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

def unix_to_year(unix_time):
    return datetime.utcfromtimestamp(int(unix_time)).strftime('%Y')

def tokenize(text):
    return [word.lower() for word in text.split() if len(word) >= 4]

for line in sys.stdin:
    row = list(csv.reader([line]))[0]
    if len(row) == 10:
        _, product_id, _, _, _, _, _, time, _, text = row
        year = unix_to_year(time)
        tokens = tokenize(text)
        print(f'{year}\t{product_id}\t{tokens}')
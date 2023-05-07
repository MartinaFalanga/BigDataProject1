#!/usr/bin/env python3
"""mapper.py"""
import sys
import datetime


for line in sys.stdin:
    line = line.strip()
    columns = line.split(",")
    try:
        productId = columns[1]
        timestamp = int(columns[6])
        text = columns[7]
        date = datetime.datetime.fromtimestamp(timestamp)
        year = date.year
        print('%i\t%s\t%s' % (year, productId, text))
    except ValueError:
        pass

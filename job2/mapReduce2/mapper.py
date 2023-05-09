#!/usr/bin/env python3
import sys


for line in sys.stdin:
    line = line.strip()
    columns = line.split(",")
    try:
        userId = columns[2]
        helpfulness_numerator = columns[3]
        helpfulness_denominator = columns[4]
        if helpfulness_denominator != "0":
            helpfulness = float(helpfulness_numerator) / float(helpfulness_denominator)
            print('%s\t%f' % (userId, helpfulness))
    except ZeroDivisionError:
        continue
    except ValueError:
        continue

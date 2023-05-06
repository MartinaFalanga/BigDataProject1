#!/usr/bin/env python3
import sys

users_helpfulness = {}
users_avg_helpfulness = {}

for line in sys.stdin:
    line = line.strip()
    userId, helpfulness = line.split('\t', 1)

    try:
        helpfulness = float(helpfulness)
        if userId not in users_helpfulness:
            users_helpfulness[userId] = {
                "sum": float(0),
                "count": 0
            }
        users_helpfulness[userId]['sum'] = users_helpfulness[userId]['sum'] + helpfulness
        users_helpfulness[userId]['count'] = users_helpfulness[userId]['count'] + 1
    except ValueError:
        continue


for userId in users_helpfulness:
    try:
        if userId not in users_avg_helpfulness:
            users_avg_helpfulness[userId] = {
                "avg": users_helpfulness[userId]['sum']/users_helpfulness[userId]['count']
            }
    except ValueError:
        continue

sorted_dictionary = sorted(users_avg_helpfulness.items(), key=lambda x: x[1]['avg'], reverse=True)

# Stampa gli utenti ordinati in base al loro apprezzamento
for key, value in sorted_dictionary:
    print('%s\t%f' % (key, value['avg']))

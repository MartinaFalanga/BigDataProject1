#!/usr/bin/env python3
import sys

current_userId = None
current_helpfulness_sum = 0
current_review_count = 0
user_helpfulness = []

for line in sys.stdin:
    line = line.strip()
    userId, helpfulness = line.split('\t', 1)

    try:
        helpfulness = float(helpfulness)
    except ValueError:
        continue

    if current_userId == userId:
        current_helpfulness_sum += helpfulness
        current_review_count += 1
    else:
        if current_userId:
            average_helpfulness = current_helpfulness_sum / current_review_count
            user_helpfulness.append((current_userId, average_helpfulness))

        current_userId = userId
        current_helpfulness_sum = helpfulness
        current_review_count = 1

if current_userId:
    average_helpfulness = current_helpfulness_sum / current_review_count
    user_helpfulness.append((current_userId, average_helpfulness))

# Ordina gli utenti in base al loro apprezzamento
sorted_user_helpfulness = sorted(user_helpfulness, key=lambda x: x[1], reverse=True)

# Stampa gli utenti ordinati in base al loro apprezzamento
for userId, avg_helpfulness in sorted_user_helpfulness:
    print('%s\t%f' % (userId, avg_helpfulness))

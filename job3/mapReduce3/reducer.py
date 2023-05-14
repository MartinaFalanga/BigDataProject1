#!/usr/bin/env python3
"""reducer.py"""

import sys
import itertools

userProducts = {}
activeUsers = {}

for line in sys.stdin:
    userId, productId = line.strip().split("\t", 1)
    try:
        if userId not in userProducts:
            userProducts[userId] = set()
        if userId in userProducts:
            userProducts[userId].add(productId)
    except ValueError:
        pass
for user in userProducts:
    if len(userProducts[user]) >= 3:
        activeUsers[user] = userProducts[user]

usersList = (tuple(x) for x in itertools.product(tuple(activeUsers.keys()), repeat=2)
             if hash(x[0]) > hash(x[1]) and len(
    activeUsers[x[0]].intersection(activeUsers[x[1]])) >= 3)

cleanedTuplesList = []
for element in usersList:
    common = set(userProducts[element[0]]).intersection(userProducts[element[1]])
    if len(common) >= 3:
        t = (element[0], element[1], common)
        cleanedTuplesList.append(t)

sortedTuplesList = sorted(cleanedTuplesList, key=lambda tup: tup[0])
for t in sortedTuplesList:
    print("User1 ID: %s User2 ID: %s Common Products: %s" % (t[0], t[1], ', '.join(t[2])))

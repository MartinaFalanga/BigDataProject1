#!/usr/bin/env python3
"""reducer.py"""

import sys
import itertools

user_products = {}
active_users = {}
cleaned_tuple = []


for line in sys.stdin:
    user_id, product_id = line.strip().split("\t", 1)
    try:
        if user_id not in user_products:
            user_products[user_id] = set()
        if user_id in user_products:
            user_products[user_id].add(product_id)
    except ValueError:
        pass
for user in user_products:
    if len(user_products[user]) >= 3:
        active_users[user] = user_products[user]

users_list = (tuple(x) for x in itertools.product(tuple(active_users.keys()), repeat=2)
             if hash(x[0]) > hash(x[1]) and len(
    active_users[x[0]].intersection(active_users[x[1]])) >= 3)

for element in users_list:
    common = set(user_products[element[0]]).intersection(user_products[element[1]])
    if len(common) >= 3:
        t = (element[0], element[1], common)
        cleaned_tuple.append(t)

sorted_tuple = sorted(cleaned_tuple, key=lambda tup: tup[0])
for t in sorted_tuple:
    print("id u1: %s id u2: %s products: %s" % (t[0], t[1], ', '.join(t[2])))

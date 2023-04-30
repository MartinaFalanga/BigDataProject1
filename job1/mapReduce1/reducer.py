#!/usr/bin/env python3
import sys
from collections import Counter, defaultdict

def top_n_items(dictionary, n):
    return dict(Counter(dictionary).most_common(n))

def print_result(year, product_reviews, top_words):
    for product_id, count in product_reviews.items():
        print(f'{year}\t{product_id}\t{count}\t{top_words[product_id]}')

current_year = None
product_reviews = Counter()
top_words = defaultdict(Counter)

for line in sys.stdin:
    year, product_id, tokens = line.strip().split('\t')
    tokens = eval(tokens)

    if year == current_year:
        product_reviews[product_id] += 1
        top_words[product_id].update(tokens)
    else:
        if current_year:
            top_10_products = top_n_items(product_reviews, 10)
            for product_id in top_10_products.keys():
                top_words[product_id] = top_n_items(top_words[product_id], 5)
            print_result(current_year, top_10_products, top_words)

        current_year = year
        product_reviews = Counter({product_id: 1})
        top_words = defaultdict(Counter)
        top_words[product_id].update(tokens)

if current_year:
    top_10_products = top_n_items(product_reviews, 10)
    for product_id in top_10_products.keys():
        top_words[product_id] = top_n_items(top_words[product_id], 5)
    print_result(current_year, top_10_products, top_words)
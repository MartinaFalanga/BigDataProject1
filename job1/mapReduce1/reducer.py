#!/usr/bin/env python3
import sys
import re
from collections import Counter

#  dizionario con doppia chiave
dict_years = {}
filtered_dict_years_top10 = {}
output_dict = {}
sorted_output_dict = {}


def clean_text(text):
    cleaned_text = re.sub(r'[^a-zA-Z\s]', '', str(text))
    return cleaned_text.lower()



def sortd_counts_word(words):
    order_dictionary = sorted(words.items(), key=lambda x: x[1], reverse=True)[:5]
    return dict(order_dictionary)

def transform_dictionary(input_dict):
    output_dict = {}
    for year, year_dict in input_dict.items():
        if year not in output_dict:
            output_dict[year] = {}
        for product_id, product_reviews in year_dict.items():
            if product_id not in output_dict[year]:
                output_dict[year][product_id] = {}
            counter_word = Counter()
            for text in product_reviews:
                text_clean = clean_text(text)
                words = text_clean.split()
                counter_word.update(words)

            sorted_word_counts = sortd_counts_word(counter_word)
            output_dict[year][product_id] = sorted_word_counts

    return output_dict





# creo un dizionario che ha come chiave l'anno.
for line in sys.stdin:
    year, product_id, text = line.strip().split('\t')
    try:
        if year not in dict_years:
            dict_years[year] = dict()
        if product_id not in dict_years[year]:
            dict_years[year][product_id] = set()
        dict_years[year][product_id].add(text)
    except ValueError:
        continue

# ordinamento dei prodotti per anno e filtraggio dei top 10
for year, product_reviews in dict_years.items():
    product_reviews_sorted_list = sorted(product_reviews.items(), key=lambda x: len(x[1]), reverse=True) # [(1234, [...]), (3453, [...])]
    filtered_dict_years_top10[year] = dict(product_reviews_sorted_list[:10])


output_dict = transform_dictionary(filtered_dict_years_top10)




for year in output_dict:
    print('%s\t%s\n' % (year, output_dict[year]))





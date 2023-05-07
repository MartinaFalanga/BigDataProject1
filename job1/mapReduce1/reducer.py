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


def sort_word_counts(word_counts):
    sorted_keys = sorted(word_counts.keys(), key=lambda x: word_counts[x], reverse=True)[:5]
    sorted_text_counts = {key: word_counts[key] for key in sorted_keys}
    return sorted_text_counts


def transform_dictionary(input_dict):
    output_dict = {}
    for year, products in input_dict.items():
        word_counts = Counter()
        product_names = list(products.keys())
        texts = products.values()
        for text in texts:
            words = clean_text(text).split()
            filtered_words = [word for word in words if len(word) >= 4]
            word_counts.update(filtered_words)
        output_dict[year] = {
            'products': product_names,
            'word_counts': word_counts.most_common(5)
        }
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
    """
    product_reviews: {
        1234: [...],
        3453: [...]
        
    }
    """
    product_reviews_sorted_list = sorted(product_reviews.items(), key=lambda x: len(x[1]), reverse=True) # [(1234, [...]), (3453, [...])]
    filtered_dict_years_top10[year] = dict(product_reviews_sorted_list[:10])


output_dict = transform_dictionary(filtered_dict_years_top10)

# # Ordinare le chiavi di output_dict
# sorted_output_dict = {year: {'products': output_dict[year]['products'],
#                              'texts': sort_text_keys(output_dict[year]['texts'])}
#                       for year in output_dict}

for year in output_dict:
    word_counts_string = ''
    for word, count in output_dict[year]['word_counts']:
        word_counts_string = f'{word}: {count}, '

    print('%s\t%s\t%s' % (year, output_dict[year]['products'], str(output_dict[year]['word_counts'])))





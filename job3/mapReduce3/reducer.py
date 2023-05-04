#!/usr/bin/env python3
"""reducer.py"""

import sys
from collections import defaultdict

# Dizionario per tenere traccia dei prodotti recensiti con score >= 4 per ogni utente
user_products = defaultdict(set)

# Leggi i dati in input dal mapper
for line in sys.stdin:
    line = line.strip()
    user_id, product_id = line.split('\t')

    # Aggiungi il prodotto all'elenco dei prodotti recensiti dall'utente
    try:
        if user_id not in user_products:
            user_products[user_id] = set()
        if user_id in user_products:
            user_products[user_id].add(product_id)
    except ValueError:
        pass

# Trovare gli utenti con gusti affini
affine_groups = defaultdict(set)

for user_id, products in user_products.items():
    for other_user_id, other_products in user_products.items():
        if user_id != other_user_id:
            common_products = products.intersection(other_products)
            if len(common_products) >= 3:
                # Ordina gli UserId e utilizza una tupla come chiave per evitare duplicati
                group_key = tuple(sorted((user_id, other_user_id)))
                affine_groups[group_key] = common_products

# Stampa i risultati
for users, shared_products in sorted(affine_groups.items(), key=lambda x: x[0][0]):
    users_str = ','.join(users)
    shared_products_str = ','.join(shared_products)
    print(f"{users_str}\t{shared_products_str}")

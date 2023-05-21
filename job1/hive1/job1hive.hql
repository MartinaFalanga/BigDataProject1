CREATE TABLE reviews (
  id STRING,
  product_id STRING,
  user_id STRING,
  helpfulness_numerator INT,
  helpfulness_denominator INT,
  score INT,
  time INT,
  text STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'dataset/dataset_doble_dimentional.csv' OVERWRITE INTO TABLE reviews;


CREATE TABLE reviews_year_text AS
SELECT
  year(from_unixtime(time)) AS year,
  product_id,
  text
FROM
  reviews;


CREATE TABLE reviews_year_product_count AS
SELECT
  year(from_unixtime(time)) AS year,
  product_id,
  COUNT(*) AS count_text
FROM
  reviews
GROUP BY
  year(from_unixtime(time)),
  product_id
ORDER BY
  year,
  count_text DESC;

CREATE TABLE top_10_products_per_year AS
SELECT year, product_id, count_text
FROM (
  SELECT year, product_id, count_text,
         ROW_NUMBER() OVER (PARTITION BY year ORDER BY count_text DESC) AS rank
  FROM reviews_year_product_count
) ranked
WHERE rank <= 10;

CREATE TABLE table_year_products_word AS
SELECT
     year,
     product_id,
     lower(word) as word
FROM reviews_year_text
LATERAL VIEW explode(split(regexp_replace(text, '[^a-zA-Z ]', ''), ' ')) exploded_table AS word;

CREATE TABLE top_10_products_year_word AS
SELECT t1.year, t1.product_id, t1.word
FROM table_year_products_word t1
JOIN top_10_products_per_year t2 ON t1.year = t2.year AND t1.product_id = t2.product_id;

CREATE TABLE top_10_products_year_word_count AS
SELECT year, product_id, word, COUNT(*) AS count_word
FROM top_10_products_year_word
WHERE LENGTH(word) >= 4
GROUP BY year, product_id, word;


CREATE TABLE top_10_products_year_word_count_order AS
SELECT year, product_id, word, count_word
FROM top_10_products_year_word_count
ORDER BY year ASC, product_id ASC, count_word DESC;


CREATE TABLE final_table AS
SELECT *
FROM (
  SELECT year, product_id, word, count_word,
         ROW_NUMBER() OVER (PARTITION BY year, product_id ORDER BY count_word DESC) as row_num
  FROM top_10_products_year_word_count_order
) subquery
WHERE row_num <= 5;



INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  year,
  CONCAT('{', CONCAT_WS(', ', COLLECT_LIST(product_id_map)), '}') AS result
FROM (
  SELECT
    year,
    CONCAT(product_id, ': [', CONCAT_WS(', ', COLLECT_LIST(CONCAT(word, ':', count_word))), ']') AS product_id_map
  FROM final_table
  GROUP BY year, product_id
) subquery
GROUP BY year;



CREATE TABLE reviews (
  Id INT,
  ProductId STRING,
  UserId STRING,
  HelpfulnessNumerator INT,
  HelpfulnessDenominator INT,
  Score INT,
  Time INT,
  Text STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'dataset/job3_prova.csv' OVERWRITE INTO TABLE reviews;



WITH user_scores AS (
  SELECT UserId, ProductId, Score
  FROM reviews
  WHERE Score >= 4
),
user_pairs AS (
  SELECT a.UserId AS user1, b.UserId AS user2, COUNT(DISTINCT a.ProductId) AS common_products
  FROM user_scores a
  JOIN user_scores b ON a.ProductId = b.ProductId AND a.UserId < b.UserId
  GROUP BY a.UserId, b.UserId
  HAVING COUNT(DISTINCT a.ProductId) >= 3
),
grouped_users AS (
  SELECT user1 AS UserId, user2, common_products
  FROM user_pairs
  UNION ALL
  SELECT user2 AS UserId, user1, common_products
  FROM user_pairs
),
grouped_products AS (
  SELECT UserId, COLLECT_SET(ProductId) AS shared_products
  FROM (
    SELECT user1 AS UserId, user2, common_products
    FROM grouped_users
    UNION ALL
    SELECT user2 AS UserId, user1, common_products
    FROM grouped_users
  ) x
  GROUP BY UserId
)

INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT DISTINCT *
FROM grouped_products
ORDER BY UserId, shared_products;
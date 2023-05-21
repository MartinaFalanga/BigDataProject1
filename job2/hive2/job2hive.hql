CREATE TABLE IF NOT EXISTS reviews (
    id INT,
    productId STRING,
    userId STRING,
    helpfulnessNumerator INT,
    helpfulnessDenominator INT,
    score INT,
    timeRew BIGINT,
    text STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'dataset/dataset_clean.csv' OVERWRITE INTO TABLE reviews;


INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    userId,
    AVG(helpfulnessNumerator / helpfulnessDenominator) AS appreciation
FROM
    reviews
GROUP BY
    userId
ORDER BY
    appreciation DESC,
    userId ASC;

DROP TABLE IF EXISTS reviews;
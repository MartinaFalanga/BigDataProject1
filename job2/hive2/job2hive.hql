CREATE TABLE IF NOT EXISTS reviews (
    Id INT,
    ProductId STRING,
    UserId STRING,
    HelpfulnessNumerator INT,
    HelpfulnessDenominator INT,
    Score INT,
    Time BIGINT,
    Text STRING
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
    UserId,
    AVG(HelpfulnessNumerator / HelpfulnessDenominator) AS Appreciation
FROM
    reviews
GROUP BY
    UserId
 ORDER BY
    Appreciation DESC;

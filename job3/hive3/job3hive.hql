CREATE TABLE IF NOT EXISTS reviews (
    id int,
    productId string,
    userId string,
    helpfullnessNumerator int,
    helpfullnessDenominator int,
    score int,
    timeRew BIGINT,
    text string
)
COMMENT 'Reviews Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'dataset/dataset_doble_dimentional.csv' overwrite INTO TABLE reviews;

CREATE TABLE couples AS
SELECT l.userId as LeftUser, l.productId, r.userId as RightUser, l.score as LeftScore, r.score as RightScore
FROM reviews l JOIN reviews r
ON l.productId = r.productId WHERE l.userId < r.userId;

CREATE TABLE couples_greater AS
SELECT *
FROM couples WHERE LeftScore >= 4 AND RightScore >= 4;

CREATE TABLE similar_couples AS
SELECT LeftUser, RightUser, COUNT(*) as repetitions
FROM couples_greater
GROUP BY LeftUser, RightUser;

CREATE TABLE similar_couples_greater AS
SELECT LeftUser,RightUser
FROM similar_couples
WHERE repetitions >= 3;

CREATE TABLE result as
SELECT c.LeftUser, productId, c.RightUser, LeftScore, RightScore
FROM couples_greater c
where exists (select s.LeftUser, s.RightUser 
              from similar_couples_greater s
              where s.LeftUser = c.LeftUser and s.RightUser = c.RightUser);

INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM result;

DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS couples;
DROP TABLE IF EXISTS couples_greater;
DROP TABLE IF EXISTS similar_couples_greater;
DROP TABLE IF EXISTS result;




# MovieRecommendation
Movie Recommention using correlation factor in Apache Hive 2.2.0

#Load Movies Data
CREATE TABLE movies1
(
id INT,
title STRING,
genres STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES(
'separatorChar' = ',',
'quoteChar' = '"',
'escapeChar' = '\\')
TBLPROPERTIES('skip.header.line.count'='1');

LOAD DATA LOCAL
INPATH '{Movies.csv file path}'
INTO TABLE movies1;

#Load Rating Data
CREATE TABLE rating
(
userid INT,
movieid INT,
rating DECIMAL(3,1),
time TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties ("skip.header.line.count"="1");


LOAD DATA LOCAL
INPATH '{Rating.csv file path}'
INTO TABLE rating;
 
#Temporary table 
CREATE TABLE output1
(
movie1 INT,
movie2 INT,
rat1 DECIMAL(3,1),
rat2 DECIMAL(3,1)
);

#Self Join on Rating table to get the pair of movies and their respectively rating by same user
INSERT INTO output1
select a.movieid,b.movieid,a.rating,b.rating
from   rating a
join   rating b
on     (a.userid = b.userid)
where a.movieid < b.movieid;

#Correlation Table
create table correlation
(
m1 INT,
m2 INT,
c DECIMAL(1,1),
cnt INT
);

#Correlation between two rating 
INSERT INTO correlation
SELECT movie1,movie2,ROUND(corr(rat1,rat2),2)as cor,COUNT(rat1) as cnt from output1
group by movie1,movie2
having cnt >1
order by cor desc;

#Inner Join of Movies and Rating Tables to show the movies which are similar to user entered movie id 
#(based on number of rating > 40 and correlation factor > 0.5)
select c.m1,c.m2,m.title from movies2 m
inner join correlation c on m.id in (c.m1,c.m2)
where {user entered movieid} in (c.m1,c.m2 ) AND c > 0.5 AND cnt > 40 AND m.title != (SELECT title FROM movies2 WHERE id = {user entered movieid});


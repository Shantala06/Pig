# Sample Data

# movieId,title,genres
# 1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
# 2,Jumanji (1995),Adventure|Children|Fantasy

# Script to count the movies by genres


register /mnt/home/shantala/flume/hadoop-training-projects/pig/movielens/piggybank-0.15.0.jar;

DEFINED myCSVLoader as org.apache.pig.piggybank.storage.CSVLoader;

data = LOAD '/user/shantala/rawdata/handson_train/movielens/latest/movies/movies.csv' using 
org.apache.pig.piggybank.storage.CSVLoader() AS (movieID:chararray,title:chararray,genres:chararray);

headless = FILTER data BY title != 'title';

projData = FOREACH headless GENERATE genres;

splitted = FOREACH projData GENERATE STRSPLIT(genres,'\\|',0) AS t;

flattened = FOREACH splitted GENERATE FLATTEN(t) AS f;

grouped = GROUP flattened BY (chararray)f;

agged = FOREACH grouped GENERATE group AS genera,COUNT(flattened) as num;

sorted = ORDER agged BY genera;

STORE sorted INTO '/user/shantala/rawdata/handson_train/movielens/genre_count/text' USING PigStorage(',');
STORE sorted INTO '/user/shantala/rawdata/handson_train/movielens/genre_count/avro' USING AvroStorage();
STORE sorted INTO '/user/shantala/rawdata/handson_train/movielens/genre_count/json' USING JsonStorage();

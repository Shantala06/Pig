-- Find the volume of stocks per stock symbol


-- Load data from HDFS to Pig
data = LOAD '/user/shantala/rawdata/handson_train/nyse' using PigStorage(',') as (exchange:chararray,stock_symbol:chararray,date:chararray,
stock_price_open:float,stock_price_high:float,stock_price_low:float,stock_price_close:float,stock_volume:int,stock_price_adj_close:float);

-- Remove header row
filtered_data = FILTER data BY exchange!='exchange';

-- project only the columns required
proj_data = FOREACH filtered_data GENERATE stock_symbol, stock_volume;

-- group the available records by the symbol
grp_data = GROUP proj_data BY stock_symbol;

-- reduce the grouped data by summing the groups
agg_data = FOREACH grp_data GENERATE group AS symbol, SUM(proj_data.stock_volume);

-- store records in the output location
STORE agg_data INTO '/user/shantala/output/hadoop_class/pig/nasdaq_stock_volume';

-- Find the overall process
illustrate agg_data;
-- Load the CSV file
salesTable = LOAD 'hdfs:///user/root/sales.csv' USING PigStorage(',') AS 
(Product:chararray,Price:chararray,Payment_Type:chararray,Name:chararray,City:chararray,State:chararray,Country:chararray);
-- Group data using the country column
groupByCountry = GROUP salesTable BY Country;
-- Generate result format
countByCountry = FOREACH groupByCountry GENERATE CONCAT((chararray)$0, CONCAT(':', (chararray)COUNT($1)));
-- Save result in HDFS folder
STORE countByCountry INTO 'hdfs:///user/sreeg/salesOutput' USING PigStorage('\t');
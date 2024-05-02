-- Load input file from HDFS
inputFileEpisode4 = LOAD 'hdfs:///user/root/inputs/episodeIV_dialogues.txt' USING PigStorage('\t') AS 
(name:chararray,line:chararray);
inputFileEpisode5 = LOAD 'hdfs:///user/root/inputs/episodeV_dialogues.txt' USING PigStorage('\t') AS 
(name:chararray,line:chararray);
inputFileEpisode6 = LOAD 'hdfs:///user/root/inputs/episodeVI_dialogues.txt' USING PigStorage('\t') AS 
(name:chararray,line:chararray);

-- Filter out first 2 lines for all 3 files
ranked4 = RANK inputFileEpisode4;
OnlyDialogues4 = FILTER ranked4 BY (rank_inputFileEpisode4 > 2);
ranked5 = RANK inputFileEpisode5;
OnlyDialogues5 = FILTER ranked5 BY (rank_inputFileEpisode5 > 2);
ranked6 = RANK inputFileEpisode6;
OnlyDialogues6 = FILTER ranked6 BY (rank_inputFileEpisode6 > 2);

-- Merge the three inputs
inputData = UNION OnlyDialogues4, OnlyDialogues5, OnlyDialogues6;

-- Group by name
grpByName = GROUP inputData BY name;

-- Count number of lines by each character
names = FOREACH grpByName GENERATE $0 as name, COUNT($1) as noOfLines;
namesOrdered = ORDER names BY noOfLines DESC;

-- Remove the old results
-- rmf hdfs:///user/sreeg/pigProjectOutput;

-- Store the result in HDFS
STORE namesOrdered INTO 'hdfs:///user/sreeg/pigProjectOutput' USING PigStorage('\t');
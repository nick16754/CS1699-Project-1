# 1699-Project-1
Project 1 for Pitt CS 1699 Cloud Computing: tinyGoogle

## To run tinyGoogle.jar: 
% bin/hadoop jar tinyGoogle.jar tinyGoogle

Output of indexing Job sent to HDFS file "output".
Final ranked output of query sent to HDFS file "results".

### Note: to run tinyGoogle.jar in MapReduce more than once, HDFS output files must be removed first using:
bin/hadoop fs -rm -r output
bin/hadoop fs -rm -r rankedOutput
bin/hadoop fs -rm -r results
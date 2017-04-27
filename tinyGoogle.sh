# Bash shell script to prompt user for tinyGoogle query
# Sends query to '/input.txt' and runs tinyGoogle.scala which uses query in '/input.txt' for Rank and Retrieval
# Written for and tested with Docker sequenceiq/spark-native-yarn image
# EOL line endings must be Unix (LF)
#!/bin/bash
echo "Welcome To tinyGoogle!"
read -p "Please enter your search query: "
echo "$REPLY" > input.txt
#hadoop fs -rm /input.txt
#hadoop fs -copyFromLocal input.txt /
echo "'/input.txt' contents in HDFS: "
#hadoop fs -cat /input.txt
echo "Running tinyGoogle with query '$REPLY' in HDFS file '/input.txt'."
./bin/spark-shell -i tinyGoogle.scala

# Bash shell script to prompt user for tinyGoogle query
# Sends query to 'input' and runs tinyGoogle.scala which uses query in 'input' for Rank and Retrieval
# Written for and tested with Docker sequenceiq/spark-native-yarn image
# EOL line endings must be Unix (LF)
#!/bin/bash
echo "Welcome To tinyGoogle!"
read -p "Please enter your search query: "
echo "$REPLY" > input
hadoop fs -rm /input
hadoop fs -copyFromLocal input /
echo "'input' contents in HDFS: "
hadoop fs -cat /input
echo "Running tinyGoogle with query '$REPLY' in 'input'."
spark-shell -i tinyGoogle.scala

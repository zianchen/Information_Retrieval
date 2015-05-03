$HADOOP_HOME/bin/hadoop dfs -rmr output

make clean
make
jar cf booleanRetrieval.jar *.java *.class

$HADOOP_HOME/bin/hadoop jar booleanRetrieval.jar Driver

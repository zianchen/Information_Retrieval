### 3. Copy input files to HDFS (stage-in)
$HADOOP_HOME/bin/hadoop dfs -rmr relevantDocScoreOutput
$HADOOP_HOME/bin/hadoop dfs -rmr topRankDocOutput
$HADOOP_HOME/bin/hadoop dfs -rmr relevantDocOutput
$HADOOP_HOME/bin/hadoop dfs -rm top20RankResult.txt

$HADOOP_HOME/bin/hadoop dfs -rmr contentOutput

make clean
make
jar cf booleanRetrieval.jar *.java *.class

$HADOOP_HOME/bin/hadoop jar booleanRetrieval.jar Driver




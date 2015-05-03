<html>
	<head>
		<title>Result Page</title>
	</head>
	<body>
	<?php
		$input = $_GET['Ori_String'];
		$input = str_replace(" ", "-", $input);
	
		$connection = ssh2_connect('user.palmetto.clemson.edu', '22'); 
		ssh2_auth_password($connection, 'wei6','Wsx891016#');

		$key =   $_POST["keyword"];
		$echocommand = 'echo ' . $key . '> /home/wei6/wei6Project3/cosinSimilaritySearch/term';
		$stream = ssh2_exec($connection,$echocommand);

		$PROJECT_HOME = "/home/wei6/wei6Project3/cosinSimilaritySearch";
		$stream = ssh2_exec($connection,'rm -rf /home/wei6/wei6Project3/cosinSimilaritySearch/contentOutput');
		$stream = ssh2_exec($connection,'rm -rf /home/wei6/wei6Project3/cosinSimilaritySearch/relevantDocScoreOutput');
		$stream = ssh2_exec($connection,'rm -rf /home/wei6/wei6Project3/cosinSimilaritySearch/topRankDocOutput');
		$stream = ssh2_exec($connection,'rm -rf /home/wei6/wei6Project3/cosinSimilaritySearch/relevantDocOutput');


		
		//$stream = ssh2_exec($connection, '$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/rank.jar yunqinz.rank data rankoutcome');
		//$stream = ssh2_exec($connection, 'cat rankoutcome');
		//$stream = ssh2_exec($connection, ' '.$PROJECT_HOME);
		//$stream = ssh2_exec($connection, 'javac /home/wei6/wei6Project3/cosinSimilaritySearch/test.java');
		//$stream = ssh2_exec($connection, 'java /home/wei6/wei6Project3/cosinSimilaritySearch/test');
		
		
		

		$stream = ssh2_exec($connection, 'java -cp /home/wei6/wei6Project3/cosinSimilaritySearch javaSSH');
		//$filename = '/home/wei6/wei6Project3/cosinSimilaritySearch/contentOutput/part-r-00000';
	   
		$isexist = false;
		while(true){

			$stream = ssh2_exec($connection, 'ls /home/wei6/wei6Project3/cosinSimilaritySearch/contentOutput/');
			
			stream_set_blocking($stream,true);
			$stream = stream_get_contents($stream);

			if($stream){
				$stream = ssh2_exec($connection, 'cat /home/wei6/wei6Project3/cosinSimilaritySearch/contentOutput/part-r-00000');
				stream_set_blocking($stream,true);
				//$stream = stream_get_contents($stream);
				while($line = fgets($stream)){
					$linearr = preg_split('/\s+/',$line);
					array_shift($linearr);
					$count = count($linearr);
					$text= array_slice($linearr, 1, $count, true);
					$text = implode(" ",$text);
					echo "<a href=\"result.php?id=$text\"> $linearr[0]</a>";
					echo "<br>";
					$output = array_slice($linearr, 1, 100, true);
					echo implode(" ", $output);
					echo "<br>";
				}
				//echo $stream;
				break;
			}
		}
		//$stream = ssh2_exec($connection, 'bash  -l /home/wei6/wei6Project3/cosinSimilaritySearch/run.bash');
		//$stream = ssh2_exec($connection, "chmod 777 nodeRun.bash");
		//if($stream = ssh2_exec($connection, './nodeRun.bash')){
		
		
		/*stream_set_blocking($stream,true);
		$content = stream_get_contents($stream);
		echo $content;*/



	//$HADOOP_HOME = "/home/yunqinz/software/hadoop/hadoop-1.2.1";
	//$stream = ssh2_exec($connection, "cp /home/chaoh/software/hadoop/hadoop-1.2.1/copy_search.sh /home/chaoh/Documents/hadoop/hadoop-1.2.1/search.sh");


	//$stream = ssh2_exec($connection, "perl -pi -e 's/&&&search_words&&&/$input/g' /home/chaoh/Documents/hadoop/hadoop-1.2.1/search.sh");
	
	//$stream = ssh2_exec($connection, "chmod 777 search.sh");
	//$stream = ssh2_exec($connection, "/home/chaoh/Documents/hadoop/hadoop-1.2.1/search.sh");
	
?>

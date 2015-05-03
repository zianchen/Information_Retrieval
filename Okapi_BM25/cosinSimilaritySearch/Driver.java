
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.util.Scanner;
import java.util.InputMismatchException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;


public class Driver extends Configured implements Tool {

	private HashMap<String,Integer>  stoppedStemmedWordMap = globalVariables.stoppedStemmedWordMap;
	private HashMap<String,Integer>  queryTermDfMap = globalVariables.queryTermDfMap;
 	private int numDocs = globalVariables.numDocs;

	private String FIRST_INPUT = "/home/wei6/wei6Project3/docIdAndTermAsTwoKeysIndex/termIndexOutput/termAsKeyIndex-r-00000";
	private String RELEVANT_DOC_OUTPUT = "/home/wei6/wei6Project3/cosinSimilaritySearch/relevantDocOutput";

	private String SECOND_INPUT = "/home/wei6/wei6Project3/docIdAndTermAsTwoKeysIndex/docIndexOutput/part-r-00000";
	private String SECOND_OUTPUT = "/home/wei6/wei6Project3/cosinSimilaritySearch/relevantDocScoreOutput";

	private String THIRD_INPUT = "/home/wei6/wei6Project3/cosinSimilaritySearch/relevantDocScoreOutput/part-r-00000";
	private String THIRD_OUTPUT = "/home/wei6/wei6Project3/cosinSimilaritySearch/topRankDocOutput";

	private String FOURTH_INPUT = "/home/wei6/wei6Project3/contentIndex/output/part-r-00000";
	private String FOURTH_OUTPUT = "/home/wei6/wei6Project3/cosinSimilaritySearch/contentOutput";

 	public static double moldQuery = 0.0;

	public int run(String[] args) throws Exception{


        Configuration conf = new Configuration();

		Job job = new Job(conf);
		FileSystem fs = FileSystem.get(conf);

		fs.delete( new Path(FIRST_INPUT + "./.*.crc"), true);
	
		job.setJobName("Hadoop job");
		job.setJarByClass(Driver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.addInputPath(job, new Path(FIRST_INPUT));
		
		TextOutputFormat.setOutputPath(job, new Path(RELEVANT_DOC_OUTPUT));
		job.waitForCompletion(true);

		/////////////////////////////////
		//must be put after the first job since queryTermDfMap was got there.
		Iterator itr = stoppedStemmedWordMap.entrySet().iterator();
		while (itr.hasNext()) {
            Map.Entry pair = (Map.Entry)itr.next();
            String key = (String)pair.getKey();
            Integer value = (Integer)pair.getValue();
            try{
            	moldQuery += (1.0 + Math.log10(value)) *(Math.log10(numDocs / (Integer)queryTermDfMap.get(key)) )*
                        (1.0 + Math.log10(value)) *(Math.log10(numDocs / (Integer)queryTermDfMap.get(key)) );
            }catch(NullPointerException e){
            	//You input non-exsiting word(s)!
            }
        }
        moldQuery = Math.sqrt(moldQuery);

		/////////////////////////////////

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2);
		FileSystem fs2 = FileSystem.get(conf2);
		fs2.delete( new Path(SECOND_INPUT + "/.*.crc"), true);
		job2.setJobName("Hadoop job");

		job2.setJarByClass(Driver.class);
		job2.setMapperClass(MyMapper2.class);
		job2.setReducerClass(MyReducer2.class);
		 
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		 
		TextInputFormat.addInputPath(job2, new Path(SECOND_INPUT));
		TextOutputFormat.setOutputPath(job2, new Path(SECOND_OUTPUT));

		job2.waitForCompletion(true);

		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3);
		FileSystem fs3 = FileSystem.get(conf3);
		fs3.delete( new Path(THIRD_INPUT + "/.*.crc"), true);
		job3.setJobName("Hadoop job");

		job3.setJarByClass(Driver.class);
		job3.setMapperClass(MyMapper3.class);
		job3.setReducerClass(MyReducer3.class);
		 
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputValueClass(Text.class);
		 
		TextInputFormat.addInputPath(job3, new Path(THIRD_INPUT));
		TextOutputFormat.setOutputPath(job3, new Path(THIRD_OUTPUT));

		job3.waitForCompletion(true);

		Configuration conf4 = new Configuration();
		Job job4 = new Job(conf4);
		FileSystem fs4 = FileSystem.get(conf4);
		fs4.delete( new Path(FOURTH_INPUT + "/.*.crc"), true);
		job4.setJobName("Hadoop job");

		job4.setJarByClass(Driver.class);
		job4.setMapperClass(MyMapper4.class);
		job4.setReducerClass(MyReducer4.class);
		 
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		 
		TextInputFormat.addInputPath(job4, new Path(FOURTH_INPUT));
		TextOutputFormat.setOutputPath(job4, new Path(FOURTH_OUTPUT));

		return job4.waitForCompletion(true) ? 0:1;


	}

	public static void main(String[] args) throws Exception {
  		ToolRunner.run(new Configuration(), new Driver(), args);
	}
}

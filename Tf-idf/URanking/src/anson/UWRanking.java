package anson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UWRanking {
    
	public static HashMap<String, Integer> querypiece = new HashMap<String,Integer>();
	public static HashMap<String, Double> docs = new HashMap<String,Double>(); 

    public static class UWRankingMap extends Mapper<Object,Text,Text,Text>{

        public void map( Object key, Text value, Context context )
            throws IOException, InterruptedException
        {
        	try{  
        		
        		String[] indexline = value.toString().split("\t");
            	Set<String> set = querypiece.keySet();
            	if(set.contains(indexline[0])){
            		Text querykey = new Text(indexline[0]);
            		Text querydoc = new Text(indexline[1]);
            		context.write(querykey,querydoc);
            	}
            }
            catch (Exception e) {
            	e.printStackTrace();
            }
        }  
    } 
    
    public static class RankCombine extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values,Context contex)
                throws IOException, InterruptedException {
        	//key is the word in both query string and in documents , and value is the corresponding value string of these keys
        	int N = 802431;
        	System.out.println("1");
        	for(Text val:values){       		
        		String docCollection[] = val.toString().split(";");
        		//docToken is a document tuple in each indexline like 2360newsML.xml@square:3;
        		for(String docToken:docCollection){
        			String tuple[] = docToken.split(":");  
        			//docNom like 2360newsML.xml@square

        			String seg[]= tuple[0].split("@");
        			Double square = Double.parseDouble(seg[1]);


        			Double Qwtf= (1+Math.log10(querypiece.get(key.toString()))); 
        			Double Query_tf_idf = Qwtf * (Math.log10(N/docCollection.length));
  
        			Double Doc_tf = 1+Math.log10(Double.valueOf(tuple[1]));
        			Double normanize = Doc_tf/square;
        			Double tf_idf = Query_tf_idf*normanize;
        
        			if(docs.containsKey(seg[0]))
						docs.put(seg[0], tf_idf+docs.get(seg[0]));//seg[0] is the file name
					else
						docs.put(seg[0], tf_idf);
        	
        		}
        	}
        	
        	//ArrayList keys = new ArrayList(docs.keySet());  
        	List<Map.Entry<String, Double>> keys = new ArrayList<Map.Entry<String, Double>>(  
        	        docs.entrySet());

        	Collections.sort(keys, new Comparator<Map.Entry<String, Double>>() {  
        	    public int compare(Map.Entry<String, Double> o1,  
        	            Map.Entry<String, Double> o2) {  
        	    	double a=Double.parseDouble(o1.getValue().toString());
	            	double b=Double.parseDouble(o2.getValue().toString());
	            	BigDecimal data1 = new BigDecimal(a); 
	            	BigDecimal data2 = new BigDecimal(b); 
	                return data2.compareTo(data1);
        	    }  
        	});  
        	
        	for (Entry<String, Double> e: keys){   
        		Text docname = new Text(e.getKey());
				Text tfidfscore = new Text(e.getValue().toString());
				contex.write(docname,tfidfscore); 
        	}   	
        }
    }

    public static class RankReduce extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values,Context contex)
                throws IOException, InterruptedException {
	        	Map<String,Double> m = new HashMap<String,Double>();
	        	   while(contex.nextKeyValue()){
	        		  m.put(contex.getCurrentKey().toString(), Double.parseDouble(contex.getCurrentValue().toString()));   
	        	   }
	        	
	            List<Map.Entry<String, Double>> record = new ArrayList<Map.Entry<String, Double>>(  
	        	        m.entrySet());
	
	        	Collections.sort(record, new Comparator<Map.Entry<String, Double>>() {  
	        	    public int compare(Map.Entry<String, Double> o1,  
	        	            Map.Entry<String, Double> o2) {  
	        	    	double a=Double.parseDouble(o1.getValue().toString());
		            	double b=Double.parseDouble(o2.getValue().toString());
		            	BigDecimal data1 = new BigDecimal(a); 
		            	BigDecimal data2 = new BigDecimal(b); 
		                return data2.compareTo(data1);
	        	    }  
	        	});  
	        	
	        	for (Entry<String, Double> e: record){   
	        		Text docname = new Text(e.getKey());
					Text tfidfscore = new Text(e.getValue().toString());
					contex.write(docname,tfidfscore); 
	        	}   
	        
        	}
        }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    	Configuration conf = new Configuration();
        Job job = new Job(conf,"RankSearch");
        
        job.setJarByClass(UWRanking.class);
        job.setMapperClass(UWRankingMap.class);
        job.setCombinerClass(RankCombine.class);
        job.setReducerClass(RankReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    	FileReader rd = new FileReader("/home/zianc/workspace/software/hadoop-1.2.1/searchinput.txt");
        BufferedReader bufr = new BufferedReader(rd);
        String inputstr = null;
        Stopwords strconvert = new Stopwords();
		while((inputstr = bufr.readLine()) != null) {
			inputstr = inputstr.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
            //remove stemmed stop words
            inputstr = strconvert.removeStemmedStopWords(inputstr);
			String[] query = inputstr.split(" ");
        	for(String word: query){ 
	        	if(querypiece.containsKey(word))
	        		querypiece.put(word, querypiece.get(word)+1);
				else
					querypiece.put(word, 1);
	        }
		}
        System.exit(job.waitForCompletion(true)?0:1);
    }
}


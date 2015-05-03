package anson;
 
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



    
public class SearchWord {
    
	public static ArrayList<String> terms1 = new ArrayList<String>(Arrays.asList(""));
	public static ArrayList<String> terms2 = new ArrayList<String>(Arrays.asList(""));
	
    public static class SearchwordMap extends Mapper<Object,Text,Text,Text>{
    	private String searchword = new String();
    	private String searchmethod = new String();
    	private int method;
        public void map( Object key, Text value, Context context )
            throws IOException, InterruptedException
        {
        	FileReader rd = new FileReader("/home/zianc/workspace/software/hadoop-1.2.1/local_input/searchinput.txt");
	        BufferedReader bufr = new BufferedReader(rd);
            
	        String inputstr = null;
            try{  
	            //pay attention, if you input more than one line of search 
            	//method and key,you will get the retrieval result for all of them
            	//没有设计搜索多个词的逻辑
            	while((inputstr = bufr.readLine()) != null) {
		        	String[] inputvalue = inputstr.split("\t");
		            searchmethod = inputvalue[0];
		            searchword = inputvalue[1];
		            System.out.println("searchmethod is: " + searchmethod +"searchword is: " + searchword);
	            	if(searchmethod.equals("uniword")){ 
	            		method = 1;	
	            	}
	            	else if(searchmethod.equals("position")){ 
	            		method = 2;
	            		System.out.println("enter position!");
	            	}
	            	else if(searchmethod.equals("biword")){
	            		method = 3;
	            	}
		            if(searchword.contains("and") || searchword.contains("or") || searchword.contains("not")){
		            	AdvanceQuery(method,searchword);
		            }
		            else{
		            	GetSearchResult(method,searchword);
		            }
		        }
            	bufr.close();
            }
            catch (Exception e) {
            	e.printStackTrace();
            }
        }  
    } 
    
    public static void GetSearchResult(int smethod,String sword){
    	// open corresponding index file and compare each index line with the searchword
        try {
	        // read file content from file
	        String indexfilepath = new String();
	        if(smethod == 1){
	        	indexfilepath = "/home/zianc/workspace/searchdir/uniword_index_1zip";	        	
	        }
	        else if(smethod == 2){
	        	indexfilepath = "/home/zianc/workspace/searchdir/position_index_1zip";	
	        }
	        else if(smethod == 3){
	        	indexfilepath = "/home/zianc/workspace/searchdir/biword_index_1zip";
	        }
	        else{
	        	System.out.println("invalid search method!");
	        	return;
	        }
	        FileReader reader = new FileReader(indexfilepath);
	        BufferedReader br = new BufferedReader(reader);
	       
	        String str = null;
	       
	        while((str = br.readLine()) != null) {
	        	  String[] indexelement = str.split("\t");
	        	  String indexkey = indexelement[0];
	              if(sword.equals(indexkey)){	            	  
	            	  FileWriter writer = new FileWriter("/home/zianc/workspace/software/hadoop-1.2.1/local_output/searchresult.txt");
	                  BufferedWriter bufferWritter = new BufferedWriter(writer);
	                  String matchvalue = indexkey + "\t" + indexelement[1];
	                  bufferWritter.write(matchvalue);
	                  bufferWritter.close();
	                  writer.close();
	              }
	        }
	       
	        br.close();
	        reader.close();
	       /*
	        // write string to file
	        FileWriter writer = new FileWriter("c://test2.txt");
	        BufferedWriter bw = new BufferedWriter(writer);
	        bw.write(sb.toString());
	       
	        bw.close();
	        writer.close();
	        */
        }
        catch (Exception e) {
        	e.printStackTrace();
        }    
    }
    
    public static void AdvanceQuery(int smethod,String sword){
    	// open corresponding index file and compare each index line with the searchword
        try {
	        // read file content from file
	        String indexfilepath = new String();
	        String[] firstposting = null;
	        String[] secondposting = null;
	        if(smethod == 1){
	        	indexfilepath = "/home/zianc/workspace/searchdir/uniword_index_1zip";	        	
	        }
	        else if(smethod == 2){
	        	indexfilepath = "/home/zianc/workspace/searchdir/position_index_1zip";	
	        }
	        else if(smethod == 3){
	        	indexfilepath = "/home/zianc/workspace/searchdir/biword_index_1zip";
	        }
	        else{
	        	System.out.println("invalid search method!");
	        	return;
	        }

	        FileReader reader = new FileReader(indexfilepath);
	        BufferedReader br = new BufferedReader(reader);	       
	        String str = null;
	        String[] querypiece = sword.split(" ");
	        int i = 0;
	        while(i < querypiece.length-1){
	        	while((str = br.readLine()) != null) {
	  	        	  //split every record in index file into key and postings
		        	  String[] indexelement = str.split("\t");
	  	        	  String indexkey = indexelement[0];
	  	              //每次计算firstkey(对应firstposting) 运算符 secondkey(对应secondposting)这样一个小组，
	  	        	  //并且把结果存入firstkey所在的位置，然后i＋2正好指向这个中间结果的位置
	  	        	  //只有第一次才会terms1.isEmpty()为真，后面terms1就一直保存中间结果
	  	        	  if(querypiece[i].equals(indexkey) && terms1.isEmpty()){	            	  
	  	            	  String indexvalue1 = indexelement[1];
	  	            	  //split postings(document sequence) by ;
	  	            	  firstposting = indexvalue1.split(";");
	  	            	  for(int p = 0; p< firstposting.length;p++){
	  	            		terms1.add(firstposting[p]);
	  	            	  } 
	  	              }
	  	              else if(querypiece[i+2].equals(indexkey)){	            	  
	  	            	  String indexvalue2 = indexelement[1];
	  	            	  //split postings(document sequence) by ;
	  	            	  secondposting = indexvalue2.split(";");
	  	            	  for(int q = 0; q< secondposting.length;q++){
	  	            		terms2.add(secondposting[q]);
	  	            	  } 
	  	              }
		        }
	        	//判断operator类型并计算中间结果，存入firstkey所在位置，即querypiece[i+2]
	        	if(querypiece[i+1].equals("and")){
	        		terms1.retainAll(terms2);

	    	        
	        	}
	        	else if(querypiece[i+1].equals("or")){
	        		int m=0;
	        		while(m<terms2.size()){
	        			if(terms1.contains(terms2.get(i))){
	        				m++;
	        			}else{
	        				terms1.add(terms2.get(m));
	        				m++;
	        			}
	        		}
	        	}
	        	else if(querypiece[i+1].equals("not")){
	        		int n=0;
	        		while(n<terms2.size()){
	        			if(terms1.contains(terms2.get(n))){
	        				terms1.remove(terms2.get(n));
	        				n++;
	        			}else{
	        				n++;
	        			}
	        		}
	        	}
	        	
	        	i = i+2; // every time get one more key and one more operator
	        }
	        
    		//final result of advanced query is in terms1
	        System.out.println(terms1);
	        
	        FileWriter writer = new FileWriter("/home/zianc/workspace/software/hadoop-1.2.1/local_output/searchresult.txt");
            BufferedWriter bw = new BufferedWriter(writer);

	        for(int s=0;s<terms1.size();s++){
	            bw.write(terms1.get(i)+";");
	            //bw.newLine();
	        }
            bw.close();
            writer.close();
            
	        br.close();
	        reader.close();  
        }
        catch (Exception e) {
        	e.printStackTrace();
        }    
    }
    
    public static class SearchwordReduce extends Reducer<Text,Text,Text,Text>{

        public void reduce(Text key, Iterable<Text> values,Context contex)
                throws IOException, InterruptedException {
        }
    }

    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    	Configuration conf = new Configuration();
        Job job = new Job(conf,"SearchWord");
        
        job.setJarByClass(SearchWord.class);
        job.setMapperClass(SearchwordMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(SearchwordReduce.class);
        job.setReducerClass(SearchwordReduce.class);
        

        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
}


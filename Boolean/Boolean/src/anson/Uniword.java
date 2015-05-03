package anson;
 
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import anson.Stemmer;
import anson.Stopwords;
    
public class Uniword {
    
    public static class UniwordMap extends Mapper<Text,BytesWritable,Text,Text>{
    	private Text valueInfo = new Text();
        private Text word = new Text();

        public void map( Text key, BytesWritable value, Context context )
            throws IOException, InterruptedException
        {
            // NOTE: the filename is the *full* path within the ZIP file
            // e.g. "subdir1/subsubdir2/Ulysses-18.txt"
            String filename = key.toString();
            //System.out.println("Map filename:" + key.toString());
            //System.out.println("Map value:" + value.toString());
            
            // We only want to process .xml files
            if ( filename.endsWith(".xml") == false )
                return;
            
	        try{    
	            // Prepare the content 
	            String rawdata = new String( value.getBytes(), "UTF-8" );
	            String content = new String();
	            Stopwords strconvert = new Stopwords();
	            
	            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse( new InputSource( new StringReader(rawdata) ) );
	
				doc.getDocumentElement().normalize();
			  
				NodeList nList = doc.getElementsByTagName("text");
			   	
			   	StringBuilder page = new StringBuilder(1024);
	
				for (int temp = 0; temp < nList.getLength(); temp++) {
			 
					Node nNode = nList.item(temp);
			  
					if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			 
						Element eElement = (Element) nNode;
						//get content between <p> and </p>
						NodeList givenNames = eElement.getElementsByTagName("p");
			 			for (int i = 0; i < givenNames.getLength(); i++) {
			 				page.append(givenNames.item(i).getTextContent());
						}
					}
				}
				//change page from StringBuilder into string
	            content = page.toString();
	            content = content.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();
	            //remove stemmed stop words
	            //content = strconvert.removeStemmedStopWords(content);
	            content = strconvert.removeStopWords(content);
	            
	            Double normanize = parse_Doc(content);
	            // Tokenize the content
	            StringTokenizer tokenizer = new StringTokenizer( content );
	            
	            while (tokenizer.hasMoreTokens()) {
	            	word.set( tokenizer.nextToken()+":"+ filename+"@"+normanize.toString());
	                valueInfo.set("1");
	                //System.out.println("Map word:" + word + "Map Value:" + valueInfo);
	                context.write(word, valueInfo);
	            } 
	        }
	        catch (Exception e) {
            	e.printStackTrace();
            }
        }  
    } 

    public static class UniwordCombiner extends Reducer<Text,Text,Text,Text>{
        
        Text info = new Text();

        public void reduce(Text key, Iterable<Text> values,Context contex)
                throws IOException, InterruptedException {
            int sum = 0;
            //System.out.println("values is***********:" + values);
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
                //System.out.println("key:" + key + "Value:" + value + " Reduce sum :" + sum);
            }
            // return the first appear position of ":" in key.toString()  
            int splitIndex = key.toString().indexOf(":");
            // change value to become this format "path:sum"
            // public String substring(int beginIndex): return substring of key.toString() from beginIndex to the end of key.toString()
            // public String substring(int beginIndex, int endIndex): return substring from beginIndex to endIndex
            info.set(key.toString().substring(splitIndex+1) +":"+ sum);
            key.set(key.toString().substring(0,splitIndex));
            contex.write(key, info);
        }
    }
 
    public static class UniwordReduce extends Reducer<Text,Text,Text,Text>{
        
        private Text result = new Text();
        
        public void reduce(Text key, Iterable<Text> values,Context contex)
	                throws IOException, InterruptedException {
		StringBuffer fileList = new StringBuffer();
		for(Text value : values) {
			fileList.append(value.toString()+";") ;
		} 
		result.set(fileList.toString());
		contex.write(key, result);
		}
    }
    
    static double parse_Doc(String s) {
		double norSquare = 0.0;
		Map<String, Integer> dw = new HashMap<String, Integer>();
		String str = s;
		String[] ch = str.split(" ");
		for (int i = 0; i < ch.length; i++) {
			Integer freq = (Integer) dw.get(ch[i]);
			dw.put(ch[i], (freq == null ? 1 : freq.intValue() + 1));
		}
		List<Map.Entry<String, Integer>> infoids = new ArrayList<Map.Entry<String, Integer>>(
				dw.entrySet());

		for (int i = 0; i < infoids.size(); i++) {
			Entry<String, Integer> id = infoids.get(i);
			double weight = 1 + Math.log(id.getValue()) / Math.log(10);
			norSquare = norSquare + weight * weight;
		}
		norSquare = Math.pow(norSquare, 0.5);
		return norSquare;
	}
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    	Configuration conf = new Configuration();
        Job job = new Job(conf,"Uniword");
        
        job.setJarByClass(Uniword.class);
        job.setMapperClass(UniwordMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(UniwordCombiner.class);
        job.setReducerClass(UniwordReduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
      
        job.setInputFormatClass(ZipFileInputFormat.class);
        
        ZipFileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

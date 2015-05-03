import java.io.IOException;

import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.BytesWritable;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import java.io.StringReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;
import java.util.TreeMap;


public  class MyMapper3
  	extends Mapper<Object, Text, NullWritable, Text> {


  	private TreeMap<Double, Text> topDocsTreeMap = new TreeMap<Double, Text>();

  	public void map( Object key, Text value, Context context )
      throws IOException, InterruptedException {

      	String[] docIdScore = value.toString().split("\\s+");
      	//must have new here
        topDocsTreeMap.put(Double.valueOf(docIdScore[1]), new Text(value)); 

        if (topDocsTreeMap.size() > 10) {
            topDocsTreeMap.remove(topDocsTreeMap.firstKey());
        }  
   	}

   	protected void cleanup(Context context) 
   			throws IOException, InterruptedException {

        for ( Text value : topDocsTreeMap.values() ) {
            context.write(NullWritable.get(), value);
        }

    }

}

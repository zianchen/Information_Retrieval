import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
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

public class MyMapper
	extends Mapper<Text, BytesWritable, Text, Text> {
	//private final static IntWritable one = new IntWritable( 1 );
	private Text word1 = new Text();
	private Text value1 = new Text();

	public void map( Text key, BytesWritable value, Context context )
	    throws IOException, InterruptedException {
	    try{

		    String filename = key.toString();
		     if ( filename.endsWith(".xml") == false )
			 return;
		    
		    String content = new String( value.getBytes(), "UTF-8" );

		    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse( new InputSource( new StringReader(content) ) );

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
		 				//blank space is needed
		 				page.append(givenNames.item(i).getTextContent() + " ");
					}
				}
			}
			
			content = page.toString();
          	word1.set( key.toString().replace("newsML.xml","") );
	        value1.set(content);
	        context.write(word1, value1);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

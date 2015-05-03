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

public class MyUniMapper
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
		    content = content.replaceAll( "[^A-Za-z \n]", " " ).toLowerCase();

		    content = Stopwords.removeStemmedStopWords(content);

		    //StringTokenizer itr = new StringTokenizer(content);
		    englishStemmer stemmer = new englishStemmer();

		    String[] array = content.split("\\s+");

		    for(int i = 0; i < array.length; i++){
		    	stemmer.setCurrent(array[i]);
		    	stemmer.stem();
		    	String stemmedWord = stemmer.getCurrent();
		    	//decided whether it is stop words after stemmered. 
		    	if(Stopwords.isStemmedStopword(stemmedWord) == true){
		    		array[i] = null;
		    	}
		    }

		    StringBuffer midResult = new StringBuffer();
			for (int i = 0; i < array.length; i++) {
				if(array[i] != null){
			   		midResult.append( array[i] );
			   		midResult.append(" ");
				}
			}

			content = midResult.toString();
			StringTokenizer itr = new StringTokenizer(content);

		    while (itr.hasMoreTokens()) {
          		stemmer.setCurrent(itr.nextToken());
          		if(stemmer.stem()){
          				String stemStr = stemmer.getCurrent();
	            		word1.set( stemStr + "," +  key.toString().replace("newsML.xml","") );
	            		// must not use key.set(...key.toString()) here
	            		// Because, the iterations will make key increase to infinite.
	            		value1.set("1");
	            		context.write(word1, value1);
          		}
        	}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

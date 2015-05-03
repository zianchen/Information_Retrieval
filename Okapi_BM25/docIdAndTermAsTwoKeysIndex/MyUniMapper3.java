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

public  class MyUniMapper3
  extends Mapper<Object, Text, Text, Text> {
  private Text keys = new Text();
  private Text values = new Text();
  public void map( Object key, Text value, Context context )
      throws IOException, InterruptedException {
        
        StringTokenizer itr = new StringTokenizer(value.toString());
         while (itr.hasMoreTokens()) {
          keys.set(itr.nextToken());
          if(itr.hasMoreTokens()){
            values.set(itr.nextToken());
          }
           context.write(keys, values);
         }

    }
}

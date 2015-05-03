import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;

public class MyReducer3
       extends Reducer<NullWritable,Text,NullWritable,Text> {


  public void reduce(NullWritable key, Iterable<Text> values,
                     Context context
                     ) throws IOException, InterruptedException {

    TreeMap<Double, Text> topDocsTreeMap = new TreeMap<Double, Text>();

      for (Text value : values) {
          String[] docIdScore = value.toString().split("\\s+");
          //must have new here
          topDocsTreeMap.put(Double.valueOf(docIdScore[1]), new Text(value));

          if (topDocsTreeMap.size() > 10) {
            topDocsTreeMap.remove(topDocsTreeMap.firstKey());
          }
      }

      for (Text value : topDocsTreeMap.values()) {
          context.write(NullWritable.get(), value);

          String[] docIdScore = value.toString().split("\\s+");
          globalVariables.rankResultMap.put(docIdScore[0], Double.valueOf(docIdScore[1]));

      }
  }

}

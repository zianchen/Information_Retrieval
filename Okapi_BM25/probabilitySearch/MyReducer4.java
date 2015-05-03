import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;

  public  class MyReducer4
         extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();
    public static HashMap<String,Double>  rankResultMap = globalVariables.rankResultMap;

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //key : term,collectionFrequency,docFrequency
      String docID = key.toString();
      //query map
     
      Double object1 = rankResultMap.get(docID);

      if (object1 != null) {
        for (Text val : values) {
          key.set(docID);
          result.set(val);
          context.write(key, result);
        }

      }

  }
}
